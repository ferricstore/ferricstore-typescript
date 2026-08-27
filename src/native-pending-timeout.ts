import { FerricStoreError, RequestTimeoutError } from "./errors.js";
import { setLongTimeout } from "./internal.js";
import type { PendingRequest } from "./native-pending-request.js";
import type { ProtocolCommand } from "./protocol.js";
import { serverResponseTimeoutMs } from "./server-response-timeout.js";

interface NativePendingTimeoutOperations {
  readonly discardRequest: (requestId: bigint) => { readonly bytes: number; readonly frames: number };
  readonly getPending: (requestId: bigint) => PendingRequest | undefined;
  readonly isWriteQueued: (requestId: bigint) => boolean;
  readonly retireConnection: (error: Error) => void;
  readonly takePending: (requestId: bigint) => PendingRequest | undefined;
}

export function timeoutNativePendingRequest(
  operations: NativePendingTimeoutOperations,
  requestId: bigint,
  timeoutMs: number
): void {
  const pending = operations.getPending(requestId);
  if (pending == null) return;
  const queued = operations.isWriteQueued(requestId);
  if (!pending.hasFlowControlCredit || queued) {
    operations.takePending(requestId)?.reject(new RequestTimeoutError(
      timeoutMs,
      queued ? "unsent" : "possibly_sent"
    ));
    return;
  }

  // The frame has entered the socket, so the server still owns this credit.
  // Retain correlation and credit until the final frame or grace expiry.
  pending.timedOut = true;
  pending.lateResponseTimer = setLongTimeout(() => {
    if (operations.getPending(requestId) !== pending) return;
    operations.retireConnection(new FerricStoreError(
      `FerricStore timed-out response did not arrive within ${timeoutMs}ms grace period`
    ));
  }, timeoutMs);
  pending.lateResponseTimer.unref();
  const discardedChunks = operations.discardRequest(requestId);
  if (discardedChunks.frames > 0) {
    pending.discardedResponseBytes =
      (pending.discardedResponseBytes ?? 0) + discardedChunks.bytes;
    pending.discardedResponseFrames =
      (pending.discardedResponseFrames ?? 0) + discardedChunks.frames;
  }
  pending.reject(new RequestTimeoutError(timeoutMs, "possibly_sent"));
}

export function nativeResponseTimeoutMs(
  command: ProtocolCommand,
  requestTimeoutMs: number
): number | undefined {
  return serverResponseTimeoutMs(requestTimeoutMs, command.serverBlockMs);
}
