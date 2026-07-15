export const MAX_TIMER_DELAY_MS = 2_147_483_647;

export interface LongTimer {
  cancel(): void;
  unref(): void;
}

export function setLongTimeout(callback: () => void, delayMs: number): LongTimer {
  let canceled = false;
  let shouldUnref = false;
  let timer: NodeJS.Timeout | undefined;
  let remainingMs = Number.isFinite(delayMs) ? Math.max(0, Math.trunc(delayMs)) : 0;

  const schedule = (): void => {
    const chunkMs = Math.min(remainingMs, MAX_TIMER_DELAY_MS);
    timer = setTimeout(() => {
      if (canceled) return;
      remainingMs -= chunkMs;
      if (remainingMs <= 0) callback();
      else schedule();
    }, chunkMs);
    if (shouldUnref) timer.unref?.();
  };
  schedule();

  return {
    cancel(): void {
      canceled = true;
      if (timer != null) clearTimeout(timer);
    },
    unref(): void {
      shouldUnref = true;
      timer?.unref?.();
    }
  };
}

export function setLongInterval(callback: () => void, intervalMs: number): LongTimer {
  let canceled = false;
  let shouldUnref = false;
  let timer: LongTimer | undefined;
  const delayMs = Number.isFinite(intervalMs) ? Math.max(1, Math.trunc(intervalMs)) : 1;

  const schedule = (): void => {
    timer = setLongTimeout(() => {
      if (canceled) return;
      try {
        callback();
      } finally {
        if (!canceled) schedule();
      }
    }, delayMs);
    if (shouldUnref) timer.unref();
  };
  schedule();

  return {
    cancel(): void {
      canceled = true;
      timer?.cancel();
    },
    unref(): void {
      shouldUnref = true;
      timer?.unref();
    }
  };
}
