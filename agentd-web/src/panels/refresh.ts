// Keeps a display-only read up to date under traffic that may never stop. A
// debounce would wait for a quiet moment that continuous traffic never gives, so
// this is a throttle: at most one read per interval, one at a time, and an
// invalidation that arrives during a read or inside the interval is kept and
// causes one more read.

export interface RefreshClock {
  now(): number;
  setTimeout(callback: () => void, ms: number): number;
  clearTimeout(handle: number): void;
}

export class RefreshThrottle {
  private pending = false;
  private running = false;
  private timer: number | null = null;
  private lastStart: number;
  private disposed = false;

  constructor(
    private readonly read: () => Promise<void>,
    private readonly intervalMs: number,
    private readonly clock: RefreshClock,
  ) {
    // The owner has just done its first read, so a change right after it waits.
    this.lastStart = clock.now();
  }

  invalidate() {
    this.pending = true;
    this.schedule();
  }

  dispose() {
    this.disposed = true;
    if (this.timer !== null) {
      this.clock.clearTimeout(this.timer);
      this.timer = null;
    }
  }

  private schedule() {
    if (this.disposed || !this.pending || this.running || this.timer !== null) {
      return;
    }
    const wait = this.lastStart + this.intervalMs - this.clock.now();
    if (wait > 0) {
      this.timer = this.clock.setTimeout(() => {
        this.timer = null;
        this.schedule();
      }, wait);
      return;
    }
    this.pending = false;
    this.running = true;
    this.lastStart = this.clock.now();
    void this.read()
      .catch((error: unknown) => {
        console.error("refreshing a view failed", error);
        // The last change may never be followed by another, so nothing else would
        // bring this view up to date. `schedule` keeps the retry to the same rate.
        this.pending = true;
      })
      .finally(() => {
        this.running = false;
        this.schedule();
      });
  }
}
