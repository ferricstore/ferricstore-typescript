import { HTTPTransportError } from "./errors.js";

interface HTTP2SlotWaiter {
  readonly abort: () => void;
  readonly reject: (reason: unknown) => void;
  readonly resolve: (release: () => void) => void;
  readonly signal: AbortSignal;
  next?: HTTP2SlotWaiter;
  previous?: HTTP2SlotWaiter;
  queued: boolean;
  settled: boolean;
}

export class HTTP2SessionRetiredError extends Error {
  constructor(message: string, cause?: unknown) {
    super(message, { cause });
    this.name = "HTTP2SessionRetiredError";
  }
}

export class HTTP2SlotPool {
  private active = 0;
  private idleCallback: (() => void) | undefined;
  private limit: number | undefined;
  private retiredError: HTTP2SessionRetiredError | undefined;
  private waiterHead: HTTP2SlotWaiter | undefined;
  private waiterTail: HTTP2SlotWaiter | undefined;

  acquire(signal: AbortSignal): Promise<() => void> {
    if (signal.aborted) return Promise.reject(signalAbortError(signal));
    if (this.retiredError != null) return Promise.reject(this.retiredError);
    if (this.limit != null && this.active < this.limit) {
      this.active += 1;
      return Promise.resolve(this.releaseOnce());
    }
    return new Promise<() => void>((resolve, reject) => {
      const waiter: HTTP2SlotWaiter = {
        abort: () => {
          if (waiter.settled) return;
          waiter.settled = true;
          this.removeWaiter(waiter);
          reject(signalAbortError(signal));
        },
        queued: true,
        reject,
        resolve,
        settled: false,
        signal
      };
      this.enqueueWaiter(waiter);
      signal.addEventListener("abort", waiter.abort, { once: true });
    });
  }

  updateLimit(limit: number): void {
    if (this.retiredError != null) return;
    this.limit = limit;
    this.drain();
  }

  retire(error: HTTP2SessionRetiredError): void {
    if (this.retiredError != null) return;
    this.retiredError = error;
    while (this.waiterHead != null) {
      const waiter = this.waiterHead;
      this.removeWaiter(waiter);
      if (waiter.settled) continue;
      waiter.settled = true;
      waiter.signal.removeEventListener("abort", waiter.abort);
      waiter.reject(error);
    }
  }

  whenIdle(callback: () => void): void {
    if (this.active === 0) callback();
    else this.idleCallback = callback;
  }

  private drain(): void {
    while (this.retiredError == null && this.limit != null && this.active < this.limit) {
      const waiter = this.waiterHead;
      if (waiter == null) return;
      this.removeWaiter(waiter);
      if (waiter.settled) continue;
      waiter.settled = true;
      waiter.signal.removeEventListener("abort", waiter.abort);
      this.active += 1;
      waiter.resolve(this.releaseOnce());
    }
  }

  private enqueueWaiter(waiter: HTTP2SlotWaiter): void {
    waiter.previous = this.waiterTail;
    if (this.waiterTail == null) this.waiterHead = waiter;
    else this.waiterTail.next = waiter;
    this.waiterTail = waiter;
  }

  private removeWaiter(waiter: HTTP2SlotWaiter): void {
    if (!waiter.queued) return;
    if (waiter.previous == null) this.waiterHead = waiter.next;
    else waiter.previous.next = waiter.next;
    if (waiter.next == null) this.waiterTail = waiter.previous;
    else waiter.next.previous = waiter.previous;
    waiter.next = undefined;
    waiter.previous = undefined;
    waiter.queued = false;
  }

  private releaseOnce(): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.active -= 1;
      this.drain();
      if (this.active === 0) {
        const callback = this.idleCallback;
        this.idleCallback = undefined;
        callback?.();
      }
    };
  }
}

export function signalAbortError(signal: AbortSignal): Error {
  return signal.reason instanceof Error
    ? signal.reason
    : new HTTPTransportError("HTTP request was aborted", { raw: signal.reason });
}
