import { JsonCodec, RawCodec, type Codec } from "./codecs.js";

type FlowValueSnapshotState =
  | { readonly bytes: Buffer; readonly ok: true }
  | { readonly error: unknown; readonly ok: false };

const flowValueSnapshots = new WeakMap<object, FlowValueSnapshotState>();

/** Encode a value at batch admission while deferring any codec error to its chunk. */
export function snapshotFlowValue(codec: Codec, value: unknown): unknown {
  if (typeof value === "object" && value != null && flowValueSnapshots.has(value)) {
    return value;
  }
  const snapshot = Object.freeze({});
  try {
    const encoded = codec.encode(value);
    flowValueSnapshots.set(snapshot, {
      bytes: codecOwnsEncodedBytes(codec, value) ? encoded : Buffer.from(encoded),
      ok: true
    });
  } catch (error) {
    flowValueSnapshots.set(snapshot, { error, ok: false });
  }
  return snapshot;
}

/** Reuse admission bytes for chunked operations; encode ordinary calls normally. */
export function encodeFlowValue(codec: Codec, value: unknown): Buffer {
  const snapshot = typeof value === "object" && value != null
    ? flowValueSnapshots.get(value)
    : undefined;
  if (snapshot == null) return codec.encode(value);
  if (!snapshot.ok) throw snapshot.error;
  return snapshot.bytes;
}

function codecOwnsEncodedBytes(codec: Codec, value: unknown): boolean {
  if (codec.constructor === JsonCodec) return true;
  return codec.constructor === RawCodec && !Buffer.isBuffer(value);
}
