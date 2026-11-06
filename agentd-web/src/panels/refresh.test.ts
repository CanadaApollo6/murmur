import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { RefreshThrottle } from "./refresh";

const INTERVAL = 2000;

function setup(answer: () => Promise<void> = () => Promise.resolve()) {
  const read = vi.fn(answer);
  const throttle = new RefreshThrottle(read, INTERVAL, {
    now: () => Date.now(),
    setTimeout: (callback, ms) => setTimeout(callback, ms) as unknown as number,
    clearTimeout: (handle) => clearTimeout(handle),
  });
  return { read, throttle };
}

describe("RefreshThrottle", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("keeps refreshing through traffic that never stops", async () => {
    const { read, throttle } = setup();
    for (let i = 0; i < 40; i += 1) {
      throttle.invalidate();
      await vi.advanceTimersByTimeAsync(500);
    }
    // Twenty seconds at one read per two seconds. A debounce would make zero.
    expect(read.mock.calls.length).toBeGreaterThanOrEqual(9);
    expect(read.mock.calls.length).toBeLessThanOrEqual(10);
  });

  it("tries a failed read again at the same rate, then goes quiet", async () => {
    let failures = 1;
    const log = vi.spyOn(console, "error").mockImplementation(() => {});
    const { read, throttle } = setup(() =>
      failures-- > 0
        ? Promise.reject(new Error("HTTP 500"))
        : Promise.resolve(),
    );
    await vi.advanceTimersByTimeAsync(INTERVAL);
    throttle.invalidate();
    await vi.advanceTimersByTimeAsync(0);
    expect(read).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(INTERVAL - 1);
    expect(read).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(read).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(read).toHaveBeenCalledTimes(2);
    expect(log).toHaveBeenCalledTimes(1);
    log.mockRestore();
  });

  it("does not try again once its owner is gone", async () => {
    const log = vi.spyOn(console, "error").mockImplementation(() => {});
    const { read, throttle } = setup(() =>
      Promise.reject(new Error("HTTP 500")),
    );
    await vi.advanceTimersByTimeAsync(INTERVAL);
    throttle.invalidate();
    await vi.advanceTimersByTimeAsync(0);
    throttle.dispose();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(read).toHaveBeenCalledTimes(1);
    log.mockRestore();
  });

  it("waits out the interval after the owner's first read", async () => {
    const { read, throttle } = setup();
    throttle.invalidate();
    await vi.advanceTimersByTimeAsync(1999);
    expect(read).toHaveBeenCalledTimes(0);
    await vi.advanceTimersByTimeAsync(2);
    expect(read).toHaveBeenCalledTimes(1);
  });

  it("reads at once after a quiet interval", async () => {
    const { read, throttle } = setup();
    await vi.advanceTimersByTimeAsync(5000);
    throttle.invalidate();
    await vi.advanceTimersByTimeAsync(0);
    expect(read).toHaveBeenCalledTimes(1);
  });

  it("does nothing without an invalidation", async () => {
    const { read, throttle } = setup();
    throttle.invalidate();
    await vi.advanceTimersByTimeAsync(2001);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(read).toHaveBeenCalledTimes(1);
  });

  it("keeps an invalidation that arrives during a read", async () => {
    let finish: () => void = () => {};
    const { read, throttle } = setup(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
    throttle.invalidate();
    await vi.advanceTimersByTimeAsync(2001);
    expect(read).toHaveBeenCalledTimes(1);
    throttle.invalidate();
    // Never two at once, however long the first one takes.
    await vi.advanceTimersByTimeAsync(10_000);
    expect(read).toHaveBeenCalledTimes(1);
    finish();
    await vi.advanceTimersByTimeAsync(0);
    expect(read).toHaveBeenCalledTimes(2);
  });

  it("keeps a pending invalidation when a read fails", async () => {
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    const { read, throttle } = setup(() => Promise.reject(new Error("down")));
    throttle.invalidate();
    await vi.advanceTimersByTimeAsync(2001);
    throttle.invalidate();
    await vi.advanceTimersByTimeAsync(2001);
    expect(read).toHaveBeenCalledTimes(2);
    expect(error).toHaveBeenCalled();
    error.mockRestore();
  });

  it("stops when disposed", async () => {
    const { read, throttle } = setup();
    throttle.invalidate();
    throttle.dispose();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(read).toHaveBeenCalledTimes(0);
  });
});
