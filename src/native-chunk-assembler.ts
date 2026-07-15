import { FerricStoreError } from "./errors.js";
import { FLAG_MORE_CHUNKS, type ResponseFrame } from "./protocol.js";

export class NativeChunkAssembler {
  private readonly chunks = new Map<string, Buffer[]>();
  private readonly keysByRequest = new Map<bigint, Set<string>>();
  private readonly bytesByKey = new Map<string, number>();
  private readonly framesByKey = new Map<string, number>();
  private totalBytes = 0;
  private totalFrames = 0;

  constructor(
    private readonly maxChunkBytes: number,
    private readonly maxChunkFrames: number,
    private readonly maxResponseBytes: number
  ) {}

  assemble(frame: ResponseFrame): ResponseFrame | undefined {
    const key = `${frame.requestId}:${frame.opcode}:${frame.laneId}`;
    const previous = this.chunks.get(key);
    if ((frame.flags & FLAG_MORE_CHUNKS) === 0 && previous == null) return frame;

    const bytes = (this.bytesByKey.get(key) ?? 0) + frame.body.byteLength;
    const frames = (this.framesByKey.get(key) ?? 0) + 1;
    this.assertLimits(bytes, frames, frame.body.byteLength);
    this.totalBytes += frame.body.byteLength;
    this.totalFrames += 1;
    this.bytesByKey.set(key, bytes);
    this.framesByKey.set(key, frames);

    if (previous == null) {
      this.chunks.set(key, [frame.body]);
      let keys = this.keysByRequest.get(frame.requestId);
      if (keys == null) {
        keys = new Set<string>();
        this.keysByRequest.set(frame.requestId, keys);
      }
      keys.add(key);
    } else {
      previous.push(frame.body);
    }
    if ((frame.flags & FLAG_MORE_CHUNKS) !== 0) return undefined;

    const chunks = this.chunks.get(key);
    if (chunks == null) return frame;
    try {
      const body = Buffer.concat(chunks, bytes);
      return { ...frame, body, bodyLength: body.byteLength };
    } finally {
      this.release(key, frame.requestId);
    }
  }

  discardRequest(requestId: bigint): { bytes: number; frames: number } {
    const keys = this.keysByRequest.get(requestId);
    if (keys == null) return { bytes: 0, frames: 0 };
    let bytes = 0;
    let frames = 0;
    for (const key of [...keys]) {
      bytes += this.bytesByKey.get(key) ?? 0;
      frames += this.framesByKey.get(key) ?? 0;
      this.release(key, requestId);
    }
    return { bytes, frames };
  }

  clear(): void {
    this.chunks.clear();
    this.keysByRequest.clear();
    this.bytesByKey.clear();
    this.framesByKey.clear();
    this.totalBytes = 0;
    this.totalFrames = 0;
  }

  private assertLimits(bytes: number, frames: number, addedBytes: number): void {
    if (bytes > this.maxChunkBytes) {
      throw new FerricStoreError(`native protocol chunked response exceeded ${this.maxChunkBytes} bytes`);
    }
    if (frames > this.maxChunkFrames) {
      throw new FerricStoreError(`native protocol chunked response exceeded ${this.maxChunkFrames} frames`);
    }
    if (bytes > this.maxResponseBytes) {
      throw new FerricStoreError(`native protocol response exceeded ${this.maxResponseBytes} bytes`);
    }
    if (this.totalBytes + addedBytes > this.maxChunkBytes) {
      throw new FerricStoreError(`native protocol buffered chunk responses exceeded ${this.maxChunkBytes} bytes`);
    }
    if (this.totalFrames + 1 > this.maxChunkFrames) {
      throw new FerricStoreError(`native protocol buffered chunk responses exceeded ${this.maxChunkFrames} frames`);
    }
  }

  private release(key: string, requestId: bigint): void {
    this.totalBytes = Math.max(0, this.totalBytes - (this.bytesByKey.get(key) ?? 0));
    this.totalFrames = Math.max(0, this.totalFrames - (this.framesByKey.get(key) ?? 0));
    this.chunks.delete(key);
    this.bytesByKey.delete(key);
    this.framesByKey.delete(key);
    const keys = this.keysByRequest.get(requestId);
    keys?.delete(key);
    if (keys?.size === 0) this.keysByRequest.delete(requestId);
  }
}
