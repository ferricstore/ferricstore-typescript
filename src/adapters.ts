import { createClient, RESP_TYPES } from "redis";
import type { Command, CommandArgument } from "./internal.js";
import { mapException } from "./errors.js";

export interface RedisCommandExecutor {
  executeCommand(...args: CommandArgument[]): Promise<unknown>;
  executePipeline?(commands: readonly Command[]): Promise<unknown[]>;
  close?(): Promise<void> | void;
}

export interface RedisAdapterOptions {
  url?: string;
  redisOptions?: Record<string, unknown>;
}

export interface NodeRedisClient {
  close?: () => Promise<unknown>;
  destroy?: () => void;
  sendCommand: (args: Array<string | Buffer>) => Promise<unknown>;
}

export class RedisAdapter implements RedisCommandExecutor {
  readonly client: NodeRedisClient;

  constructor(client: NodeRedisClient) {
    this.client = client;
  }

  static async fromUrl(url: string, options: Record<string, unknown> = {}): Promise<RedisAdapter> {
    const rawClient = createClient({
      ...options,
      RESP: 3,
      url
    }).withTypeMapping({
      [RESP_TYPES.BLOB_STRING]: Buffer
    });

    const client = rawClient as unknown as NodeRedisClient & { connect: () => Promise<unknown> };
    await client.connect();
    return new RedisAdapter(client);
  }

  async executeCommand(...args: CommandArgument[]): Promise<unknown> {
    try {
      return await this.client.sendCommand(args.map(toRedisArgument));
    } catch (error) {
      throwMapped(error);
    }
  }

  async executePipeline(commands: readonly Command[]): Promise<unknown[]> {
    try {
      return await Promise.all(commands.map((command) => this.client.sendCommand(command.map(toRedisArgument))));
    } catch (error) {
      throwMapped(error);
    }
  }

  async close(): Promise<void> {
    if (this.client.close != null) {
      await this.client.close();
      return;
    }
    this.client.destroy?.();
  }
}

function toRedisArgument(value: CommandArgument): string | Buffer {
  if (Buffer.isBuffer(value)) {
    return value;
  }
  if (value == null) {
    return "";
  }
  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }
  return String(value);
}

function throwMapped(error: unknown): never {
  const mapped = mapException(error);
  throw mapped instanceof Error ? mapped : error;
}
