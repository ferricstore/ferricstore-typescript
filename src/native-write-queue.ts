import type net from "node:net";
import type tls from "node:tls";
import { Buffer } from "node:buffer";
import { OverloadedError } from "./errors.js";
import {
  possiblySentConnectionClosedError,
  unsentConnectionClosedError
} from "./adapter-connection-errors.js";

interface QueuedWrite {
  readonly bytes: number;
  frame?: Buffer;
  readonly requestId: bigint;
}

export class NativeWriteQueue {
  private readonly queuedWrites: QueuedWrite[] = [];
  private readonly queuedWritesByRequest = new Map<bigint, QueuedWrite>();
  private queuedWriteBytes = 0;
  private queuedWriteHead = 0;
  private queuedWriteTombstones = 0;
  private writeBlocked = false;

  constructor(
    private readonly socket: net.Socket | tls.TLSSocket,
    private readonly maxQueuedWriteBytes: number,
    private readonly isClosed: () => boolean,
    private readonly hasPending: (requestId: bigint) => boolean,
    private readonly rejectPending: (requestId: bigint, error: Error) => void
  ) {}

  cancel(requestId: bigint): void {
    const queued = this.queuedWritesByRequest.get(requestId);
    if (queued?.frame == null) return;
    queued.frame = undefined;
    this.queuedWritesByRequest.delete(requestId);
    this.queuedWriteBytes = Math.max(0, this.queuedWriteBytes - queued.bytes);
    this.queuedWriteTombstones += 1;
    this.compactTombstones();
  }

  clear(): void {
    this.queuedWrites.length = 0;
    this.queuedWritesByRequest.clear();
    this.queuedWriteBytes = 0;
    this.queuedWriteHead = 0;
    this.queuedWriteTombstones = 0;
    this.writeBlocked = false;
  }

  flush(): void {
    if (this.isClosed()) {
      this.clear();
      return;
    }
    this.writeBlocked = false;
    while (!this.writeBlocked && this.queuedWriteHead < this.queuedWrites.length) {
      const queued = this.queuedWrites[this.queuedWriteHead];
      this.queuedWriteHead += 1;
      if (queued?.frame == null) {
        if (queued != null) {
          this.queuedWriteTombstones = Math.max(0, this.queuedWriteTombstones - 1);
        }
        continue;
      }
      const frame = queued.frame;
      queued.frame = undefined;
      this.queuedWritesByRequest.delete(queued.requestId);
      this.queuedWriteBytes = Math.max(0, this.queuedWriteBytes - queued.bytes);
      if (this.hasPending(queued.requestId)) this.writeFrame(queued.requestId, frame);
    }
    if (this.queuedWriteHead === this.queuedWrites.length) {
      this.queuedWrites.length = 0;
      this.queuedWriteHead = 0;
      this.queuedWriteTombstones = 0;
    } else if (this.queuedWriteHead >= 1_024 && this.queuedWriteHead * 2 >= this.queuedWrites.length) {
      this.queuedWrites.splice(0, this.queuedWriteHead);
      this.queuedWriteHead = 0;
    }
  }

  has(requestId: bigint): boolean {
    return this.queuedWritesByRequest.has(requestId);
  }

  rejectQueued(error: Error): void {
    for (const requestId of [...this.queuedWritesByRequest.keys()]) {
      this.rejectPending(requestId, error);
      this.cancel(requestId);
    }
  }

  write(requestId: bigint, frame: Buffer): void {
    if (!this.writeBlocked) {
      this.writeFrame(requestId, frame);
      return;
    }
    if (frame.byteLength > this.maxQueuedWriteBytes - this.queuedWriteBytes) {
      this.rejectPending(requestId, new OverloadedError(
        "FerricStore client write queue is full",
        { reason: "client_write_queue_full" }
      ));
      return;
    }
    const queued: QueuedWrite = { bytes: frame.byteLength, frame, requestId };
    this.queuedWrites.push(queued);
    this.queuedWritesByRequest.set(requestId, queued);
    this.queuedWriteBytes += queued.bytes;
  }

  private compactTombstones(): void {
    if (this.queuedWritesByRequest.size === 0) {
      this.queuedWrites.length = 0;
      this.queuedWriteHead = 0;
      this.queuedWriteTombstones = 0;
      return;
    }
    const remaining = this.queuedWrites.length - this.queuedWriteHead;
    if (this.queuedWriteTombstones < 1_024 || this.queuedWriteTombstones * 2 < remaining) return;
    let writeIndex = 0;
    for (let index = this.queuedWriteHead; index < this.queuedWrites.length; index += 1) {
      const queued = this.queuedWrites[index];
      if (queued?.frame != null) {
        this.queuedWrites[writeIndex] = queued;
        writeIndex += 1;
      }
    }
    this.queuedWrites.length = writeIndex;
    this.queuedWriteHead = 0;
    this.queuedWriteTombstones = 0;
  }

  private writeFrame(requestId: bigint, frame: Buffer): void {
    try {
      const accepted = this.socket.write(frame, (error) => {
        if (error != null) {
          this.rejectPending(requestId, possiblySentConnectionClosedError(error));
        }
      });
      if (!accepted) this.writeBlocked = true;
    } catch (error) {
      this.rejectPending(requestId, unsentConnectionClosedError(error));
    }
  }
}
