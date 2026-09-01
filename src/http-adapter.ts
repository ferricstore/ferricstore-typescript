import type { Command, CommandArgument } from "./internal.js";
import type { CommandExecutor } from "./adapter-types.js";
import type { ExecutePipelineOptions } from "./pipeline-execution.js";
import { classifyServerError, FerricStoreError, HTTPTransportError } from "./errors.js";
import { assertHTTPCommandSupported } from "./http-command-policy.js";
import { decodeHTTPEnvelope, encodeHTTPCommands } from "./http-envelope.js";
import { buildProtocolCommand } from "./protocol.js";
import { COMMAND_OPCODES, OPCODES } from "./protocol-constants.js";
import {
  normalizeHTTPOptions,
  type HTTPAdapterConfig,
  type HTTPAdapterOptions
} from "./http-options.js";
import { HTTPTransport } from "./http-transport.js";
import { combinedServerBlockMs, serverResponseTimeoutMs } from "./server-response-timeout.js";

interface HTTPResult {
  readonly error?: unknown;
  readonly status: "error" | "ok";
  readonly value?: unknown;
}

export class HTTPAdapter implements CommandExecutor {
  readonly #config: HTTPAdapterConfig;
  readonly #transport: HTTPTransport;

  static async fromUrl(url: string, options: HTTPAdapterOptions = {}): Promise<HTTPAdapter> {
    return new HTTPAdapter(normalizeHTTPOptions(url, options));
  }

  private constructor(config: HTTPAdapterConfig) {
    this.#config = config;
    this.#transport = new HTTPTransport(config);
  }

  async executeCommand(...args: CommandArgument[]): Promise<unknown> {
    return await this.executeCommandArgs(args);
  }

  async executeCommandArgs(args: readonly CommandArgument[]): Promise<unknown> {
    const [result] = await this.executeBatch([args]);
    if (result == null) throw new HTTPTransportError("HTTP response omitted a command result");
    if (result.status === "error") throw commandError(result.error);
    return result.value;
  }

  async executePipeline(
    commands: readonly Command[],
    options: ExecutePipelineOptions = {}
  ): Promise<unknown[]> {
    const results = await this.executeBatch(commands);
    const values = results.map((result) => result.status === "ok" ? result.value : commandError(result.error));
    if (options.throwOnItemError !== false) {
      const failure = values.find((value) => value instanceof Error);
      if (failure instanceof Error) throw failure;
    }
    return values;
  }

  async executeFusedPipeline(
    commands: readonly Command[],
    options: ExecutePipelineOptions = {}
  ): Promise<unknown[]> {
    return await this.executePipeline(commands, options);
  }

  async close(): Promise<void> {
    await this.#transport.close();
  }

  private async executeBatch(commands: readonly (readonly CommandArgument[])[]): Promise<HTTPResult[]> {
    let body: Buffer;
    let serverBlockMs: number | undefined;
    try {
      if (commands.length > this.#config.maxBatchItems) {
        throw new HTTPTransportError("HTTP command batch exceeds maxBatchItems");
      }
      for (const command of commands) assertHTTPCommandSupported(command[0]);
      const prepared = commands.map((command) => prepareHTTPCommand(command, this.#config.maxRequestBytes));
      body = encodeHTTPCommands(prepared.map((command) => command.encoded), this.#config.maxRequestBytes);
      if (body.byteLength > this.#config.maxRequestBytes) {
        throw new HTTPTransportError("HTTP command request exceeds maxRequestBytes");
      }
      serverBlockMs = combinedServerBlockMs(prepared.map((command) => command.serverBlockMs));
    } catch (error) {
      throw unsentHTTPError(error);
    }
    const response = await this.#transport.post(
      body,
      serverResponseTimeoutMs(this.#config.timeoutMs, serverBlockMs)
    );
    let envelope: Record<string, unknown> = {};
    try {
      if (response.body.byteLength > 0) envelope = decodeHTTPEnvelope(response.body);
    } catch (error) {
      if (response.status >= 200 && response.status < 300) throw error;
    }
    if (response.status < 200 || response.status >= 300) throw topLevelError(response.status, envelope,
      response.headers["retry-after"]);
    if (envelope.encoding !== "ferricstore-json-v1") {
      throw new HTTPTransportError("HTTP response has an unsupported encoding");
    }
    if (!Array.isArray(envelope.results) || envelope.results.length !== commands.length) {
      throw new HTTPTransportError("HTTP response result count does not match the command batch");
    }
    return envelope.results.map((result) => validatedResult(result));
  }
}

const commandNamesByOpcode = new Map<number, string>(
  Object.entries(COMMAND_OPCODES).map(([name, opcode]) => [opcode, name])
);

interface PreparedHTTPCommand {
  readonly encoded: unknown;
  readonly serverBlockMs?: number;
}

function prepareHTTPCommand(
  command: readonly CommandArgument[],
  maxRequestBytes: number
): PreparedHTTPCommand {
  const protocol = buildProtocolCommand(command, maxRequestBytes, false);
  if (protocol.opcode === OPCODES.commandExec) {
    return { encoded: command, serverBlockMs: protocol.serverBlockMs };
  }
  const name = commandNamesByOpcode.get(protocol.opcode);
  if (name == null) throw new HTTPTransportError(`HTTP command has unknown opcode ${protocol.opcode}`);
  return {
    encoded: { command: name, opcode: protocol.opcode, payload: protocol.payload ?? {} },
    serverBlockMs: protocol.serverBlockMs
  };
}

function validatedResult(value: unknown): HTTPResult {
  if (!isRecord(value)) throw new HTTPTransportError("HTTP response has an invalid result item");
  if (value.status === "ok" && Object.hasOwn(value, "value")) {
    return { status: "ok", value: value.value };
  }
  if (value.status === "error" && isRecord(value.error)) {
    return { error: value.error, status: "error" };
  }
  throw new HTTPTransportError("HTTP response has an invalid result item");
}

function commandError(value: unknown): FerricStoreError {
  const message = isRecord(value) && typeof value.message === "string"
    ? value.message
    : "FerricStore command failed";
  const code = isRecord(value) && typeof value.code === "string" ? value.code : undefined;
  return classifyServerError(message, value, undefined, code);
}

function topLevelError(status: number, envelope: Record<string, unknown>, retryAfter: string | undefined): Error {
  const details = isRecord(envelope.error) ? envelope.error : {};
  const code = typeof details.code === "string" ? details.code.toLowerCase() : undefined;
  const message = typeof details.message === "string"
    ? details.message
    : `HTTP command request failed with status ${status}`;
  const retryAfterMs = retryAfterMilliseconds(retryAfter);
  const ambiguousTimeout = status === 408 || code === "request_timeout";
  const safeToRetry = !ambiguousTimeout && (
    details.safe_to_retry === true || definitelyRejectedHTTPStatus(status)
  );
  return new HTTPTransportError(message, {
    raw: details,
    retryable: status === 408 || status === 425 || status === 429 || status >= 500,
    retryAfterMs,
    requestDisposition: "possibly_sent",
    safeToRetry,
    statusCode: status
  });
}

function definitelyRejectedHTTPStatus(status: number): boolean {
  return status === 400 || status === 401 || status === 403 || status === 404 ||
    status === 405 || status === 406 || status === 411 || status === 413 ||
    status === 414 || status === 415 || status === 422 || status === 426 || status === 431;
}

function unsentHTTPError(error: unknown): HTTPTransportError {
  if (error instanceof HTTPTransportError && error.requestDisposition === "unsent") return error;
  const message = error instanceof Error ? error.message : String(error);
  return new HTTPTransportError(message, {
    cause: error,
    raw: error instanceof FerricStoreError ? error.raw ?? error : error,
    requestDisposition: "unsent"
  });
}

function retryAfterMilliseconds(value: string | undefined): number | undefined {
  if (value == null) return undefined;
  if (/^\d+$/u.test(value)) {
    const seconds = Number.parseInt(value, 10);
    const milliseconds = seconds * 1_000;
    return Number.isSafeInteger(milliseconds) ? milliseconds : undefined;
  }
  const deadline = Date.parse(value);
  if (!Number.isFinite(deadline)) return undefined;
  const milliseconds = Math.max(0, deadline - Date.now());
  return Number.isSafeInteger(milliseconds) ? milliseconds : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value != null && !Array.isArray(value);
}
