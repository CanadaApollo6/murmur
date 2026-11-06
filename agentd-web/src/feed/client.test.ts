import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { type ClientDeps, FeedClient } from "./client";
import { SimMailroom } from "./simulator";

// Lets a test make the reducer throw, which no valid frame should ever do.
const fault = vi.hoisted(() => ({ throwOnFrames: false }));
vi.mock("./reducer", async (original) => {
  const actual = await original<typeof import("./reducer")>();
  return {
    ...actual,
    reduce: (...args: Parameters<typeof actual.reduce>) => {
      if (fault.throwOnFrames && args[1].type === "frames") {
        throw new Error("the reducer threw");
      }
      return actual.reduce(...args);
    },
  };
});

const TOKEN = "ab".repeat(32);

class FakeSocket {
  onopen: ((event: Event) => void) | null = null;
  onmessage: ((event: MessageEvent) => void) | null = null;
  onclose: ((event: CloseEvent) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  closed = false;

  constructor(
    readonly url: string,
    readonly protocols: string[],
  ) {}

  open() {
    this.onopen?.({} as Event);
  }

  send(frame: unknown) {
    this.onmessage?.({ data: JSON.stringify(frame) } as MessageEvent);
  }

  drop() {
    this.onclose?.({} as CloseEvent);
  }

  close() {
    this.closed = true;
  }
}

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), { status });
}

function setup(respond: (target: string) => Promise<Response>) {
  const sockets: FakeSocket[] = [];
  const requests: { target: string; init: RequestInit }[] = [];
  const rejected = vi.fn();
  const deps: ClientDeps = {
    token: TOKEN,
    host: "127.0.0.1:7434",
    createSocket: (url, protocols) => {
      const socket = new FakeSocket(url, protocols);
      sockets.push(socket);
      return socket;
    },
    fetch: (target, init) => {
      requests.push({ target, init });
      return respond(target);
    },
    now: () => Date.now(),
    random: () => 0.5,
    setTimeout: (callback, ms) => setTimeout(callback, ms) as unknown as number,
    clearTimeout: (handle) => clearTimeout(handle),
    log: () => {},
    onTokenRejected: rejected,
  };
  const client = new FeedClient(deps);
  return { client, sockets, requests, rejected };
}

/** Lets promise chains started by a fake response finish. */
async function settle() {
  for (let i = 0; i < 20; i += 1) {
    await Promise.resolve();
  }
  await vi.advanceTimersByTimeAsync(0);
}

describe("FeedClient", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("offers both subprotocols, with no query before any state", () => {
    const { client, sockets } = setup(() =>
      Promise.reject(new Error("unused")),
    );
    client.start();
    expect(sockets[0]!.url).toBe("ws://127.0.0.1:7434/events");
    expect(sockets[0]!.protocols).toEqual([
      "murmur.events.v1",
      `murmur.token.${TOKEN}`,
    ]);
  });

  it("applies everything received before it reconnects with the anchor", async () => {
    const { client, sockets } = setup(() =>
      Promise.reject(new Error("unused")),
    );
    const room = new SimMailroom();
    client.start();
    const first = sockets[0]!;
    first.open();
    first.send(room.snapshot());
    expect(client.getState().link).toBe("live");
    // These sit in the flush queue when the socket drops.
    first.send(room.accept("human", "agent:A"));
    first.send(room.accept("human", "agent:A"));
    first.drop();
    expect(client.getState().link).toBe("disconnected");
    expect(client.getState().cursor).toBe(2);
    await vi.advanceTimersByTimeAsync(1000);
    expect(sockets[1]!.url).toBe(
      "ws://127.0.0.1:7434/events?cursor=2&nonce=nonce-2",
    );
    const second = sockets[1]!;
    second.open();
    second.send(room.nullSnapshot(2));
    const missed = room.accept("human", "agent:B");
    second.send(missed);
    await vi.advanceTimersByTimeAsync(100);
    expect(client.getState().cursor).toBe(3);
    expect(Object.keys(client.getState().edges)).toHaveLength(2);
  });

  it("backs off, and resets once a snapshot arrives", async () => {
    const { client, sockets } = setup(() => Promise.reject(new Error("down")));
    const room = new SimMailroom();
    client.start();
    sockets[0]!.drop();
    await settle();
    expect(client.getRetry().attempt).toBe(1);
    await vi.advanceTimersByTimeAsync(499);
    expect(sockets).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(2);
    expect(sockets).toHaveLength(2);
    sockets[1]!.drop();
    await settle();
    expect(client.getRetry().attempt).toBe(2);
    await vi.advanceTimersByTimeAsync(999);
    expect(sockets).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(2);
    expect(sockets).toHaveLength(3);
    sockets[2]!.open();
    sockets[2]!.send(room.snapshot());
    expect(client.getRetry().attempt).toBe(0);
    sockets[2]!.drop();
    expect(client.getRetry().attempt).toBe(1);
  });

  it("caps the backoff", async () => {
    const { client, sockets } = setup(() => Promise.reject(new Error("down")));
    client.start();
    for (let i = 0; i < 12; i += 1) {
      sockets.at(-1)!.drop();
      await settle();
      await vi.advanceTimersByTimeAsync(30_001);
    }
    expect(sockets).toHaveLength(13);
  });

  it("tells a rejected token apart from a server that is down", async () => {
    const { client, sockets, rejected } = setup(() =>
      Promise.resolve(json(401, {})),
    );
    client.start();
    sockets[0]!.drop();
    await settle();
    expect(client.getState().link).toBe("rejected");
    expect(rejected).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(120_000);
    expect(sockets).toHaveLength(1);
    client.retryNow();
    expect(sockets).toHaveLength(1);
  });

  it("reports a feed refused by a reachable server", async () => {
    const { client, sockets } = setup(() =>
      Promise.resolve(json(200, { rows: [], next_before_msg_seq: null })),
    );
    client.start();
    sockets[0]!.drop();
    await settle();
    expect(client.getState().link).toBe("disconnected");
    expect(client.getRetry().refused).toBe(true);
  });

  it("does not probe after an established connection drops", async () => {
    const { client, sockets, requests } = setup(() =>
      Promise.reject(new Error("unused")),
    );
    client.start();
    sockets[0]!.open();
    sockets[0]!.send(new SimMailroom().snapshot());
    sockets[0]!.drop();
    await settle();
    expect(requests).toHaveLength(0);
  });

  it("ignores frames from a socket it has replaced", async () => {
    const { client, sockets } = setup(() =>
      Promise.reject(new Error("unused")),
    );
    const room = new SimMailroom();
    client.start();
    sockets[0]!.open();
    sockets[0]!.send(room.snapshot());
    sockets[0]!.drop();
    await vi.advanceTimersByTimeAsync(1000);
    sockets[0]!.send(room.accept("human", "agent:A"));
    await vi.advanceTimersByTimeAsync(100);
    expect(client.getState().cursor).toBe(0);
  });

  it("gives the probe a request timeout", async () => {
    const abort = new AbortController();
    const timeout = vi
      .spyOn(AbortSignal, "timeout")
      .mockReturnValue(abort.signal);
    const { client, sockets, requests } = setup(() => new Promise(() => {}));
    client.start();
    sockets[0]!.drop();
    await settle();
    expect(requests[0]!.target).toBe("/mail?limit=1");
    expect(timeout).toHaveBeenCalledWith(30_000);
    expect(requests[0]!.init.signal).toBe(abort.signal);
    timeout.mockRestore();
  });

  it("schedules the backoff when the probe's request is aborted", async () => {
    const abort = new AbortController();
    const timeout = vi
      .spyOn(AbortSignal, "timeout")
      .mockReturnValue(abort.signal);
    const { client, sockets } = setup(
      () =>
        new Promise<Response>((_resolve, reject) => {
          abort.signal.addEventListener("abort", () =>
            reject(abort.signal.reason),
          );
        }),
    );
    client.start();
    sockets[0]!.drop();
    await settle();
    expect(sockets).toHaveLength(1);
    abort.abort(new DOMException("timed out", "TimeoutError"));
    await settle();
    expect(client.getRetry().attempt).toBe(1);
    expect(client.getRetry().refused).toBe(false);
    await vi.advanceTimersByTimeAsync(501);
    expect(sockets).toHaveLength(2);
    timeout.mockRestore();
  });

  describe("a lost mail frame", () => {
    function connected() {
      const harness = setup(() => Promise.reject(new Error("unused")));
      const room = new SimMailroom();
      room.accept("human", "agent:A");
      harness.client.start();
      harness.sockets[0]!.open();
      harness.sockets[0]!.send(room.snapshot());
      return { ...harness, room };
    }

    it("closes the socket and reconnects without the anchor", async () => {
      const { client, sockets, room } = connected();
      room.accept("human", "agent:A"); // Never delivered.
      sockets[0]!.send(room.accept("human", "agent:A"));
      await vi.advanceTimersByTimeAsync(100);
      expect(sockets[0]!.closed).toBe(true);
      expect(client.getState().link).toBe("disconnected");
      expect(client.getRetry().problem).toContain("missed");
      await vi.advanceTimersByTimeAsync(501);
      expect(sockets[1]!.url).toBe("ws://127.0.0.1:7434/events");
    });

    it("backs off and says so when the socket closes before the gap's flush", async () => {
      const { client, sockets, room } = connected();
      const attempts: number[] = [];
      for (let cycle = 0; cycle < 5; cycle += 1) {
        const socket = sockets.at(-1)!;
        const count = sockets.length;
        room.accept("human", "agent:A"); // Never delivered.
        socket.send(room.accept("human", "agent:A"));
        socket.drop(); // The frame after the gap is still waiting to be flushed.
        expect(client.getState().link).toBe("disconnected");
        expect(client.getRetry().problem).toContain("missed");
        const { attempt } = client.getRetry();
        attempts.push(attempt);
        // Exactly one retry, and not before its backoff.
        const ceiling = Math.min(30_000, 500 * 2 ** (attempt - 1));
        await vi.advanceTimersByTimeAsync(ceiling * 0.8 - 1);
        expect(sockets).toHaveLength(count);
        await vi.advanceTimersByTimeAsync(ceiling * 0.4 + 2);
        expect(sockets).toHaveLength(count + 1);
        const next = sockets.at(-1)!;
        expect(next.url).toBe("ws://127.0.0.1:7434/events");
        next.open();
        next.send(room.snapshot());
      }
      expect(attempts).toEqual([1, 2, 3, 4, 5]);
      await vi.advanceTimersByTimeAsync(300_000);
      expect(sockets).toHaveLength(6);
    });

    it("costs one full snapshot, then no reconnect loop", async () => {
      const { client, sockets, room } = connected();
      room.accept("human", "agent:A"); // Never delivered.
      sockets[0]!.send(room.accept("human", "agent:A"));
      await vi.advanceTimersByTimeAsync(601);
      expect(sockets).toHaveLength(2);
      sockets[1]!.open();
      sockets[1]!.send(room.snapshot());
      expect(client.getState().resyncNeeded).toBe(false);
      expect(client.getRetry()).toEqual({
        attempt: 0,
        refused: false,
        problem: null,
      });
      // The hole is behind the new cursor now, so nothing leads back to it.
      for (let i = 0; i < 50; i += 1) {
        sockets[1]!.send(room.accept("human", "agent:A"));
        await vi.advanceTimersByTimeAsync(1000);
      }
      await vi.advanceTimersByTimeAsync(300_000);
      expect(sockets).toHaveLength(2);
      expect(sockets[1]!.closed).toBe(false);
      expect(client.getState().cursor).toBe(room.head());
      expect(
        Object.values(client.getState().edges).reduce(
          (n, edge) => n + edge.queued,
          0,
        ),
      ).toBe(room.edges().reduce((n, edge) => n + edge.queued, 0));
    });

    it("backs off if the server keeps skipping, instead of spinning", async () => {
      const { client, sockets, room } = connected();
      const delays: number[] = [];
      let last = Date.now();
      for (let round = 0; round < 7; round += 1) {
        const socket = sockets.at(-1)!;
        room.accept("human", "agent:A"); // Never delivered.
        socket.send(room.accept("human", "agent:A"));
        await vi.advanceTimersByTimeAsync(50);
        const count = sockets.length;
        last = Date.now();
        while (sockets.length === count) {
          await vi.advanceTimersByTimeAsync(100);
        }
        delays.push(Date.now() - last);
        sockets.at(-1)!.open();
        sockets.at(-1)!.send(room.snapshot());
      }
      expect(delays[0]).toBeLessThanOrEqual(600);
      expect(delays[6]).toBeGreaterThan(delays[0]! * 16);
      expect(client.getState().link).toBe("live");
    });

    it("starts from the shortest backoff again after a quiet minute", async () => {
      const { sockets, room } = connected();
      room.accept("human", "agent:A");
      sockets[0]!.send(room.accept("human", "agent:A"));
      await vi.advanceTimersByTimeAsync(601);
      sockets[1]!.open();
      sockets[1]!.send(room.snapshot());
      await vi.advanceTimersByTimeAsync(61_000);
      room.accept("human", "agent:A");
      sockets[1]!.send(room.accept("human", "agent:A"));
      await vi.advanceTimersByTimeAsync(601);
      expect(sockets).toHaveLength(3);
    });
  });

  describe("a frame this page cannot use", () => {
    afterEach(() => {
      fault.throwOnFrames = false;
    });

    function resumable() {
      const harness = setup(() => Promise.reject(new Error("unused")));
      const room = new SimMailroom();
      room.accept("human", "agent:A");
      harness.client.start();
      harness.sockets[0]!.open();
      harness.sockets[0]!.send(room.snapshot());
      return { ...harness, room };
    }

    function send(socket: FakeSocket, data: string) {
      socket.onmessage?.({ data } as MessageEvent);
    }

    it.each([
      ["a snapshot with no fields", '{"event":"snapshot"}'],
      ["text that is not JSON", "{not json"],
      ["JSON with no event", "[1,2]"],
      ["a mail frame with no seq", '{"event":"mail","id":"m"}'],
      ["an activity frame with no text", '{"event":"activity","agent":"a"}'],
    ])(
      "%s: reconnects without the anchor, and says so",
      async (_name, data) => {
        const { client, sockets, room } = resumable();
        expect(() => send(sockets[0]!, data)).not.toThrow();
        expect(sockets[0]!.closed).toBe(true);
        expect(client.getRetry().problem).toContain("agentd sent");
        expect(client.getState().link).toBe("disconnected");
        await vi.advanceTimersByTimeAsync(601);
        expect(sockets[1]!.url).toBe("ws://127.0.0.1:7434/events");
        sockets[1]!.open();
        sockets[1]!.send(room.snapshot());
        expect(client.getState().link).toBe("live");
        expect(client.getRetry().problem).toBeNull();
      },
    );

    it("recovers when a malformed snapshot is the first frame", async () => {
      const { client, sockets } = setup(() =>
        Promise.reject(new Error("unused")),
      );
      client.start();
      sockets[0]!.open();
      send(sockets[0]!, '{"event":"snapshot"}');
      expect(client.getState().link).toBe("disconnected");
      await vi.advanceTimersByTimeAsync(601);
      expect(sockets).toHaveLength(2);
    });

    it("ignores an event kind from a newer server", async () => {
      const { client, sockets } = resumable();
      send(sockets[0]!, '{"event":"from_the_future","anything":[1]}');
      await vi.advanceTimersByTimeAsync(601);
      expect(sockets).toHaveLength(1);
      expect(sockets[0]!.closed).toBe(false);
      expect(client.getRetry().problem).toBeNull();
      expect(client.getState().link).toBe("live");
    });

    it("a reducer throw on a live socket reconnects without the anchor", async () => {
      const { client, sockets, room } = resumable();
      fault.throwOnFrames = true;
      sockets[0]!.send(room.accept("human", "agent:A"));
      await vi.advanceTimersByTimeAsync(100);
      expect(sockets[0]!.closed).toBe(true);
      expect(client.getRetry().problem).toContain("could not apply");
      fault.throwOnFrames = false;
      await vi.advanceTimersByTimeAsync(601);
      expect(sockets[1]!.url).toBe("ws://127.0.0.1:7434/events");
    });

    it("a reducer throw inside onclose does not kill the reconnect", async () => {
      const { client, sockets, room } = resumable();
      sockets[0]!.send(room.accept("human", "agent:A"));
      fault.throwOnFrames = true;
      expect(() => sockets[0]!.drop()).not.toThrow();
      fault.throwOnFrames = false;
      expect(client.getState().link).toBe("disconnected");
      expect(client.getRetry().problem).toContain("could not apply");
      await vi.advanceTimersByTimeAsync(601);
      expect(sockets[1]!.url).toBe("ws://127.0.0.1:7434/events");
    });

    it("a reducer that keeps throwing on the snapshot backs off and stays visible", async () => {
      const { client, sockets, room } = resumable();
      fault.throwOnFrames = true;
      for (let i = 0; i < 5; i += 1) {
        sockets.at(-1)!.send(room.snapshot());
        expect(client.getRetry().problem).toContain("could not apply");
        const count = sockets.length;
        while (sockets.length === count) {
          await vi.advanceTimersByTimeAsync(100);
        }
        sockets.at(-1)!.open();
      }
      expect(client.getRetry().attempt).toBe(0 + 5);
    });

    it("a malformed /mail page fails that read, not the page", async () => {
      const room = new SimMailroom();
      for (let i = 0; i < 1001; i += 1) {
        room.accept("human", "agent:A", "held");
      }
      const { client, sockets } = setup(() =>
        Promise.resolve(json(200, { nope: true })),
      );
      client.start();
      sockets[0]!.open();
      sockets[0]!.send(room.snapshot());
      await settle();
      expect(sockets[0]!.closed).toBe(false);
      expect(client.getState().stuck.outstanding).toBeNull();
      expect(client.getState().stuck.dirty).toBe(true);
    });
  });

  it("refetches a view-mode list with the bearer token, and retries a failure", async () => {
    const room = new SimMailroom();
    for (let i = 0; i < 1001; i += 1) {
      room.accept("human", "agent:A", "held");
    }
    let answer: () => Promise<Response> = () =>
      Promise.resolve(json(500, "nope"));
    const { client, sockets, requests } = setup(() => answer());
    client.start();
    sockets[0]!.open();
    sockets[0]!.send(room.snapshot());
    expect(requests).toHaveLength(1);
    expect(requests[0]!.target).toBe(
      "/mail?states=held%2Cuncertain&limit=1000",
    );
    expect(requests[0]!.init.headers).toEqual({
      Authorization: `Bearer ${TOKEN}`,
    });
    await settle();
    // Failed: dirty again, and the next attempt waits for the rate limit.
    expect(client.getState().stuck.dirty).toBe(true);
    expect(requests).toHaveLength(1);
    answer = () =>
      Promise.resolve(
        json(200, {
          rows: room.read(["held", "uncertain"]),
          next_before_msg_seq: 1,
        }),
      );
    await vi.advanceTimersByTimeAsync(1001);
    expect(requests).toHaveLength(2);
    await settle();
    expect(client.getState().stuck.dirty).toBe(false);
    expect(client.getState().stuck.outstanding).toBeNull();
  });

  it("sends the suppressed refetch from a timer when no other event arrives", async () => {
    const room = new SimMailroom();
    for (let i = 0; i < 1001; i += 1) {
      room.accept("human", "agent:A", "held");
    }
    const { client, sockets, requests } = setup(() =>
      Promise.resolve(json(200, { rows: [], next_before_msg_seq: null })),
    );
    client.start();
    sockets[0]!.open();
    sockets[0]!.send(room.snapshot());
    await settle();
    sockets[0]!.send(room.move("m000001", "dead"));
    await vi.advanceTimersByTimeAsync(100);
    expect(requests).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1000);
    expect(requests).toHaveLength(2);
  });

  it("a 401 on a data read rejects the token", async () => {
    const { client, sockets, rejected } = setup(() =>
      Promise.resolve(json(401, {})),
    );
    client.start();
    sockets[0]!.open();
    sockets[0]!.send(new SimMailroom().snapshot());
    await expect(client.message("m1")).rejects.toThrow("rejected");
    expect(client.getState().link).toBe("rejected");
    expect(rejected).toHaveBeenCalledTimes(1);
    expect(sockets[0]!.closed).toBe(true);
  });

  it("stop closes the socket and stops retrying", async () => {
    const { client, sockets } = setup(() => Promise.reject(new Error("down")));
    client.start();
    client.stop();
    expect(sockets[0]!.closed).toBe(true);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(sockets).toHaveLength(1);
  });
});
