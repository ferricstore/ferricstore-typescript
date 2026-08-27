import http from "node:http";
import http2 from "node:http2";
import https from "node:https";
import { Buffer } from "node:buffer";
import type { IncomingHttpHeaders, IncomingMessage } from "node:http";
import type {
  ClientHttp2Session,
  ClientHttp2Stream,
  IncomingHttpHeaders as Http2Headers,
  Settings as Http2Settings
} from "node:http2";
import { HTTPTransportError, RequestTimeoutError } from "./errors.js";
import {
  HTTP2SessionRetiredError,
  HTTP2SlotPool,
  signalAbortError
} from "./http2-slot-pool.js";
import type { HTTPAdapterConfig } from "./http-options.js";
import { setLongTimeout } from "./internal-timers.js";

export interface HTTPResponse {
  readonly body: Buffer;
  readonly headers: Readonly<Record<string, string>>;
  readonly status: number;
}

export class HTTPTransport {
  readonly #httpAgent: http.Agent;
  readonly #httpsAgent: https.Agent;
  readonly #allSessions = new Set<ClientHttp2Session>();
  readonly #sessions = new Map<string, ClientHttp2Session>();
  readonly #sessionSlots = new WeakMap<ClientHttp2Session, HTTP2SlotPool>();
  readonly #requests = new Set<AbortController>();
  #closed = false;

  constructor(private readonly config: HTTPAdapterConfig) {
    this.#httpAgent = new http.Agent({
      keepAlive: true,
      maxFreeSockets: config.maxConnections,
      maxSockets: config.maxConnections,
      maxTotalSockets: config.maxConnections
    });
    this.#httpsAgent = new https.Agent({
      ...config.tlsOptions,
      keepAlive: true,
      maxFreeSockets: config.maxConnections,
      maxSockets: config.maxConnections,
      maxTotalSockets: config.maxConnections
    });
  }

  async post(body: Buffer, timeoutMs: number | undefined): Promise<HTTPResponse> {
    if (this.#closed) throw new HTTPTransportError("HTTP transport is closed");
    const controller = new AbortController();
    this.#requests.add(controller);
    const timer = timeoutMs == null
      ? undefined
      : setLongTimeout(() => controller.abort(requestTimeoutReason), timeoutMs);
    timer?.unref();
    try {
      return await this.request(this.config.commandUrl, "POST", body, 0, controller.signal);
    } catch (error) {
      if (controller.signal.aborted) {
        if (controller.signal.reason instanceof HTTPTransportError) throw controller.signal.reason;
        throw new RequestTimeoutError(timeoutMs ?? this.config.timeoutMs, "possibly_sent", {
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
      timer?.cancel();
      this.#requests.delete(controller);
    }
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    const error = new HTTPTransportError("HTTP transport is closed");
    for (const controller of this.#requests) controller.abort(error);
    this.#httpAgent.destroy();
    this.#httpsAgent.destroy();
    for (const session of this.#allSessions) session.destroy();
    this.#allSessions.clear();
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
    const headers: http2.OutgoingHttpHeaders = {
      ...this.requestHeaders(body),
      ":authority": url.host,
      ":method": method,
      ":path": `${url.pathname}${url.search}`,
      ":scheme": url.protocol.slice(0, -1)
    };
    if (signal.aborted) throw signalAbortError(signal);
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const session = this.session(url);
      let release: () => void;
      try {
        release = await this.slotPool(session).acquire(signal);
      } catch (error) {
        if (attempt === 0 && error instanceof HTTP2SessionRetiredError && !signal.aborted) continue;
        throw error;
      }
      try {
        let stream: ClientHttp2Stream;
        try {
          stream = session.request(headers);
        } catch (error) {
          if (attempt === 0 && retryableSessionOpenError(error) && !signal.aborted) {
            this.retireSession(url.origin, session, true, error);
            continue;
          }
          throw error;
        }
        try {
          return await this.collectHttp2(stream, body, signal);
        } catch (error) {
          if (attempt === 0 && refusedStreamError(error) && !signal.aborted) continue;
          throw error;
        }
      } finally {
        release();
      }
    }
    throw new HTTPTransportError("HTTP/2 session could not accept the request");
  }

  private async collectHttp2(
    stream: ClientHttp2Stream,
    body: Buffer | undefined,
    signal: AbortSignal
  ): Promise<HTTPResponse> {
    return await new Promise<HTTPResponse>((resolve, reject) => {
      let responseHeaders: Http2Headers = {};
      const abort = (): void => stream.close(http2.constants.NGHTTP2_CANCEL);
      signal.addEventListener("abort", abort, { once: true });
      stream.once("response", (value) => responseHeaders = value);
      stream.once("error", reject);
      void collectBody(stream, 0, {}, this.config.maxResponseBytes).then((response) => {
        const status = Number(responseHeaders[":status"] ?? 0);
        resolve({ body: response.body, headers: normalizeHeaders(responseHeaders), status });
      }, reject).finally(() => signal.removeEventListener("abort", abort));
      if (signal.aborted) abort();
      else stream.end(body);
    });
  }

  private session(url: URL): ClientHttp2Session {
    const origin = url.origin;
    const current = this.#sessions.get(origin);
    if (current != null && !current.closed && !current.destroyed) return current;
    const session = http2.connect(origin, {
      ...this.config.tlsOptions,
      settings: { enablePush: false }
    });
    const slots = new HTTP2SlotPool();
    this.#allSessions.add(session);
    this.#sessionSlots.set(session, slots);
    session.on("remoteSettings", (settings: Http2Settings) => {
      const remoteLimit = settings.maxConcurrentStreams;
      const limit = typeof remoteLimit === "number" && Number.isFinite(remoteLimit)
        ? Math.max(0, Math.floor(remoteLimit))
        : this.config.maxConnections;
      slots.updateLimit(Math.min(this.config.maxConnections, limit));
    });
    session.on("error", (error) => this.retireSession(origin, session, true, error));
    session.once("goaway", () => {
      this.retireSession(
        origin,
        session,
        "when_idle",
        new HTTP2SessionRetiredError("HTTP/2 session received GOAWAY")
      );
    });
    session.once("close", () => {
      // Node 22 can emit `close` for a gracefully retired GOAWAY session while
      // its underlying socket still needs an explicit destroy during transport
      // shutdown. Retain non-destroyed sessions until `close()` owns that work.
      if (session.destroyed) this.#allSessions.delete(session);
      this.retireSession(origin, session);
    });
    this.#sessions.set(origin, session);
    return session;
  }

  private retireSession(
    origin: string,
    session: ClientHttp2Session,
    destroy: boolean | "when_idle" = false,
    cause?: unknown
  ): void {
    if (this.#sessions.get(origin) === session) this.#sessions.delete(origin);
    const slots = this.#sessionSlots.get(session);
    slots?.retire(
      cause instanceof HTTP2SessionRetiredError
        ? cause
        : new HTTP2SessionRetiredError("HTTP/2 session is unavailable", cause)
    );
    if (destroy === true && !session.destroyed) session.destroy();
    else if (destroy === "when_idle") {
      slots?.whenIdle(() => {
        if (!session.destroyed) session.destroy();
      });
    }
  }

  private slotPool(session: ClientHttp2Session): HTTP2SlotPool {
    const slots = this.#sessionSlots.get(session);
    if (slots == null) throw new HTTP2SessionRetiredError("HTTP/2 session is unavailable");
    return slots;
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

const requestTimeoutReason = Symbol("ferricstore-http-request-timeout");

function retryableSessionOpenError(error: unknown): boolean {
  if (typeof error !== "object" || error == null || !("code" in error)) return false;
  const code = (error as { readonly code?: unknown }).code;
  return code === "ERR_HTTP2_GOAWAY_SESSION" || code === "ERR_HTTP2_INVALID_SESSION";
}

function refusedStreamError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  return (error as Error & { readonly code?: string }).code === "ERR_HTTP2_STREAM_ERROR"
    && error.message.includes("NGHTTP2_REFUSED_STREAM");
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
  const result = Object.create(null) as Record<string, string>;
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
