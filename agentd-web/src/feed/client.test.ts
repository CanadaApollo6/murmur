import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { type ClientDeps, FeedClient } from "./client";
import { SimMailroom } from "./simulator";

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

  it("survives a malformed message", () => {
    const { client, sockets } = setup(() =>
      Promise.reject(new Error("unused")),
    );
    client.start();
    sockets[0]!.open();
    sockets[0]!.onmessage?.({ data: "{not json" } as MessageEvent);
    sockets[0]!.send(new SimMailroom().snapshot());
    expect(client.getState().link).toBe("live");
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
