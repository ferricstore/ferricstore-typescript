import http from "node:http";
import http2 from "node:http2";
import https from "node:https";
import { Buffer } from "node:buffer";
import type { IncomingHttpHeaders, IncomingMessage } from "node:http";
import type { ClientHttp2Session, IncomingHttpHeaders as Http2Headers } from "node:http2";
import { HTTPTransportError, RequestTimeoutError } from "./errors.js";
import type { HTTPAdapterConfig } from "./http-options.js";

export interface HTTPResponse {
  readonly body: Buffer;
  readonly headers: Readonly<Record<string, string>>;
  readonly status: number;
}

export class HTTPTransport {
  readonly #httpAgent: http.Agent;
  readonly #httpsAgent: https.Agent;
  readonly #sessions = new Map<string, ClientHttp2Session>();
  #closed = false;

  constructor(private readonly config: HTTPAdapterConfig) {
    this.#httpAgent = new http.Agent({ keepAlive: true, maxSockets: config.maxConnections });
    this.#httpsAgent = new https.Agent({
      ...config.tlsOptions,
      keepAlive: true,
      maxSockets: config.maxConnections
    });
  }

  async post(body: Buffer): Promise<HTTPResponse> {
    if (this.#closed) throw new HTTPTransportError("HTTP transport is closed");
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.config.timeoutMs);
    try {
      return await this.request(this.config.commandUrl, "POST", body, 0, controller.signal);
    } catch (error) {
      if (controller.signal.aborted) {
        throw new RequestTimeoutError(this.config.timeoutMs, "possibly_sent", {
          cause: error,
          raw: { retryable: true, safe_to_retry: false }
        });
      }
      if (error instanceof HTTPTransportError || error instanceof RequestTimeoutError) throw error;
      throw new HTTPTransportError("HTTP command request failed", {
        cause: error,
        retryable: true,
        safeToRetry: false
      });
    } finally {
      clearTimeout(timer);
    }
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#httpAgent.destroy();
    this.#httpsAgent.destroy();
    for (const session of this.#sessions.values()) session.destroy();
    this.#sessions.clear();
  }

  private async request(
    url: URL,
    method: "GET" | "POST",
    body: Buffer | undefined,
    redirects: number,
    signal: AbortSignal
  ): Promise<HTTPResponse> {
    const response = this.config.http2
      ? await this.http2Request(url, method, body, signal)
      : await this.http1Request(url, method, body, signal);
    if (![301, 302, 303, 307, 308].includes(response.status)) return response;
    if (redirects >= this.config.maxRedirects) throw new HTTPTransportError("too many HTTP redirects");
    const location = response.headers.location;
    if (location == null || location === "") throw new HTTPTransportError("HTTP redirect has no location");
    const target = new URL(location, url);
    if (target.protocol !== "http:" && target.protocol !== "https:") {
      throw new HTTPTransportError("HTTP redirect has an unsupported scheme");
    }
    const switchToGet = [301, 302, 303].includes(response.status);
    return await this.request(target, switchToGet ? "GET" : method, switchToGet ? undefined : body,
      redirects + 1, signal);
  }

  private async http1Request(
    url: URL,
    method: "GET" | "POST",
    body: Buffer | undefined,
    signal: AbortSignal
  ): Promise<HTTPResponse> {
    const request = url.protocol === "https:" ? https.request : http.request;
    const agent = url.protocol === "https:" ? this.#httpsAgent : this.#httpAgent;
    const headers = this.requestHeaders(body);
    return await new Promise<HTTPResponse>((resolve, reject) => {
      const outgoing = request(url, { agent, headers, method, signal }, (response) => {
        void this.collectHttp1(response).then(resolve, reject);
      });
      outgoing.once("error", reject);
      outgoing.end(body);
    });
  }

  private async collectHttp1(response: IncomingMessage): Promise<HTTPResponse> {
    const declared = headerValue(response.headers["content-length"]);
    if (declared != null && Number(declared) > this.config.maxResponseBytes) {
      response.destroy();
      throw new HTTPTransportError("HTTP response exceeds maxResponseBytes");
    }
    return await collectBody(
      response,
      response.statusCode ?? 0,
      normalizeHeaders(response.headers),
      this.config.maxResponseBytes
    );
  }

  private async http2Request(
    url: URL,
    method: "GET" | "POST",
    body: Buffer | undefined,
    signal: AbortSignal
  ): Promise<HTTPResponse> {
    const session = this.session(url);
    const headers: http2.OutgoingHttpHeaders = {
      ...this.requestHeaders(body),
      ":authority": url.host,
      ":method": method,
      ":path": `${url.pathname}${url.search}`,
      ":scheme": url.protocol.slice(0, -1)
    };
    return await new Promise<HTTPResponse>((resolve, reject) => {
      const stream = session.request(headers);
      let responseHeaders: Http2Headers = {};
      const abort = (): void => stream.close(http2.constants.NGHTTP2_CANCEL);
      signal.addEventListener("abort", abort, { once: true });
      stream.once("response", (value) => responseHeaders = value);
      stream.once("error", reject);
      void collectBody(stream, 0, {}, this.config.maxResponseBytes).then((response) => {
        const status = Number(responseHeaders[":status"] ?? 0);
        resolve({ body: response.body, headers: normalizeHeaders(responseHeaders), status });
      }, reject).finally(() => signal.removeEventListener("abort", abort));
      stream.end(body);
    });
  }

  private session(url: URL): ClientHttp2Session {
    const origin = url.origin;
    const current = this.#sessions.get(origin);
    if (current != null && !current.closed && !current.destroyed) return current;
    const session = http2.connect(origin, this.config.tlsOptions);
    session.on("error", () => undefined);
    session.once("close", () => this.#sessions.delete(origin));
    this.#sessions.set(origin, session);
    return session;
  }

  private requestHeaders(body: Buffer | undefined): Record<string, string | number> {
    const headers = { ...this.config.headers };
    delete headers["content-length"];
    delete headers["content-type"];
    return {
      ...headers,
      accept: "application/json",
      ...(body == null ? {} : { "content-length": body.byteLength, "content-type": "application/json" })
    };
  }
}

async function collectBody(
  source: NodeJS.ReadableStream & AsyncIterable<Uint8Array | string>,
  status: number,
  headers: Readonly<Record<string, string>>,
  maximum: number
): Promise<HTTPResponse> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of source) {
    const bytes = Buffer.from(chunk);
    size += bytes.byteLength;
    if (size > maximum) {
      const destroy = (source as { destroy?: () => void }).destroy;
      if (destroy != null) destroy.call(source);
      throw new HTTPTransportError("HTTP response exceeds maxResponseBytes");
    }
    chunks.push(bytes);
  }
  return { body: Buffer.concat(chunks, size), headers, status };
}

function normalizeHeaders(headers: IncomingHttpHeaders | Http2Headers): Record<string, string> {
  const result: Record<string, string> = {};
  for (const [name, raw] of Object.entries(headers)) {
    const value = headerValue(raw);
    if (value != null) result[name.toLowerCase()] = value;
  }
  return result;
}

function headerValue(value: string | string[] | number | undefined): string | undefined {
  if (Array.isArray(value)) return value.join(", ");
  return value == null ? undefined : String(value);
}
