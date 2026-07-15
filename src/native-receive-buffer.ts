import { FerricStoreError } from "./errors.js";
import { HEADER_SIZE, tryDecodeFrame, type ResponseFrame } from "./protocol.js";

const FRAME_ASSEMBLY_PAGE_BYTES = 64 * 1024;

class NativeFrameAssembly {
  readonly pages: Buffer[] = [];
  private pageBytes = 0;
  private receivedBytes = 0;

  constructor(private readonly frameLength: number) {}

  get complete(): boolean {
    return this.receivedBytes === this.frameLength;
  }

  append(chunk: Buffer): number {
    let sourceOffset = 0;
    while (sourceOffset < chunk.byteLength && this.receivedBytes < this.frameLength) {
      let page = this.pages[this.pages.length - 1];
      if (page == null || this.pageBytes === page.byteLength) {
        page = Buffer.allocUnsafe(Math.min(
          FRAME_ASSEMBLY_PAGE_BYTES,
          this.frameLength - this.receivedBytes
        ));
        this.pages.push(page);
        this.pageBytes = 0;
      }
      const bytes = Math.min(
        chunk.byteLength - sourceOffset,
        page.byteLength - this.pageBytes,
        this.frameLength - this.receivedBytes
      );
      chunk.copy(page, this.pageBytes, sourceOffset, sourceOffset + bytes);
      this.pageBytes += bytes;
      this.receivedBytes += bytes;
      sourceOffset += bytes;
    }
    return sourceOffset;
  }

  finish(): Buffer {
    if (!this.complete) throw new FerricStoreError("native receive buffer frame is incomplete");
    const first = this.pages[0];
    if (first == null) throw new FerricStoreError("native receive buffer frame is empty");
    return this.pages.length === 1 ? first : Buffer.concat(this.pages, this.frameLength);
  }
}

export class NativeReceiveBuffer {
  private assembly?: NativeFrameAssembly;
  private readonly chunks: Buffer[] = [];
  private bufferedBytes = 0;
  private chunkIndex = 0;
  private chunkOffset = 0;

  append(chunk: Buffer): void {
    if (chunk.byteLength === 0) return;
    const consumed = this.assembly?.append(chunk) ?? 0;
    if (consumed < chunk.byteLength) {
      const remaining = consumed === 0 ? chunk : chunk.subarray(consumed);
      this.chunks.push(remaining);
      this.bufferedBytes += remaining.byteLength;
    }
  }

  clear(): void {
    this.assembly = undefined;
    this.chunks.length = 0;
    this.bufferedBytes = 0;
    this.chunkIndex = 0;
    this.chunkOffset = 0;
  }

  nextFrame(maxFrameBytes: number): ResponseFrame | undefined {
    const assembly = this.assembly;
    if (assembly != null) {
      if (!assembly.complete) return undefined;
      this.assembly = undefined;
      return this.decodeFrame(assembly.finish(), maxFrameBytes);
    }
    if (this.bufferedBytes < HEADER_SIZE) return undefined;
    const header = this.peek(HEADER_SIZE);
    tryDecodeFrame(header, maxFrameBytes);
    const frameLength = HEADER_SIZE + header.readUInt32BE(20);
    if (this.bufferedBytes < frameLength) {
      this.assembly = new NativeFrameAssembly(frameLength);
      this.drainInto(this.assembly);
      return undefined;
    }
    return this.decodeFrame(this.read(frameLength), maxFrameBytes);
  }

  private decodeFrame(encoded: Buffer, maxFrameBytes: number): ResponseFrame {
    const parsed = tryDecodeFrame(encoded, maxFrameBytes);
    if (parsed?.rest.byteLength !== 0) {
      throw new FerricStoreError("invalid buffered native protocol frame");
    }
    return parsed.frame;
  }

  private drainInto(assembly: NativeFrameAssembly): void {
    while (this.bufferedBytes > 0) {
      const chunk = this.chunks[this.chunkIndex];
      if (chunk == null) throw new FerricStoreError("native receive buffer underflow");
      const bytes = assembly.append(chunk.subarray(this.chunkOffset));
      if (bytes <= 0) throw new FerricStoreError("native receive buffer frame overflow");
      this.bufferedBytes -= bytes;
      this.chunkOffset += bytes;
      if (this.chunkOffset === chunk.byteLength) {
        this.chunkIndex += 1;
        this.chunkOffset = 0;
      }
    }
    this.compact();
  }

  private peek(length: number): Buffer {
    const first = this.chunks[this.chunkIndex];
    if (first == null) throw new FerricStoreError("native receive buffer underflow");
    const firstBytes = first.byteLength - this.chunkOffset;
    if (firstBytes >= length) return first.subarray(this.chunkOffset, this.chunkOffset + length);

    const out = Buffer.allocUnsafe(length);
    let outputOffset = 0;
    let chunkIndex = this.chunkIndex;
    let chunkOffset = this.chunkOffset;
    while (outputOffset < length) {
      const chunk = this.chunks[chunkIndex];
      if (chunk == null) throw new FerricStoreError("native receive buffer underflow");
      const bytes = Math.min(length - outputOffset, chunk.byteLength - chunkOffset);
      chunk.copy(out, outputOffset, chunkOffset, chunkOffset + bytes);
      outputOffset += bytes;
      chunkIndex += 1;
      chunkOffset = 0;
    }
    return out;
  }

  private read(length: number): Buffer {
    const out = this.peek(length);
    let remaining = length;
    while (remaining > 0) {
      const chunk = this.chunks[this.chunkIndex];
      if (chunk == null) throw new FerricStoreError("native receive buffer underflow");
      const bytes = Math.min(remaining, chunk.byteLength - this.chunkOffset);
      remaining -= bytes;
      this.chunkOffset += bytes;
      if (this.chunkOffset === chunk.byteLength) {
        this.chunkIndex += 1;
        this.chunkOffset = 0;
      }
    }
    this.bufferedBytes -= length;
    this.compact();
    return out;
  }

  private compact(): void {
    if (this.chunkIndex === this.chunks.length) {
      this.chunks.length = 0;
      this.chunkIndex = 0;
      this.chunkOffset = 0;
    } else if (this.chunkIndex >= 1_024 && this.chunkIndex * 2 >= this.chunks.length) {
      this.chunks.splice(0, this.chunkIndex);
      this.chunkIndex = 0;
    }
  }
}
