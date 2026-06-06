export interface Codec<T = unknown> {
  encode(value: T | null | undefined): Buffer;
  decode(value: Buffer | null | undefined): T | null;
}

export class RawCodec implements Codec<Buffer | string | Uint8Array | null> {
  encode(value: Buffer | string | Uint8Array | null | undefined): Buffer {
    if (value == null) {
      return Buffer.alloc(0);
    }
    if (Buffer.isBuffer(value)) {
      return value;
    }
    if (value instanceof Uint8Array) {
      return Buffer.from(value);
    }
    if (typeof value === "string") {
      return Buffer.from(value);
    }
    throw new TypeError("RawCodec accepts Buffer, Uint8Array, string, null, or undefined");
  }

  decode(value: Buffer | null | undefined): Buffer | null {
    return value ?? null;
  }
}

export class JsonCodec<T = unknown> implements Codec<T> {
  encode(value: T | null | undefined): Buffer {
    return Buffer.from(JSON.stringify(value ?? null));
  }

  decode(value: Buffer | null | undefined): T | null {
    if (value == null) {
      return null;
    }
    return JSON.parse(value.toString("utf8")) as T;
  }
}
