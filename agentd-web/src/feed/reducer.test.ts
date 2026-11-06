import { describe, expect, it } from "vitest";
import {
  type ClientState,
  LIST_BOUND,
  LIST_STATES,
  type ListName,
  REFETCH_INTERVAL_MS,
  edgeKey,
  eventsTarget,
  initialState,
  listTarget,
  planFetches,
  reduce,
} from "./reducer";
import { SimMailroom } from "./simulator";
import type { DeliveryState, EdgeCounts, Frame, MailFrame } from "./types";

const A = "agent:A";
const B = "agent:B";
const C = "agent:C";

function apply(state: ClientState, ...frames: Frame[]): ClientState {
  return reduce(state, { type: "frames", frames, now: 0 });
}

function fresh(room: SimMailroom): ClientState {
  return apply(
    reduce(initialState(), { type: "socket_open" }),
    room.snapshot(),
  );
}

function sortedEdges(edges: Iterable<EdgeCounts>): EdgeCounts[] {
  return [...edges].sort((a, b) =>
    edgeKey(a.from, a.to) < edgeKey(b.from, b.to) ? -1 : 1,
  );
}

function expectEdgesEqualFresh(state: ClientState, room: SimMailroom) {
  expect(sortedEdges(Object.values(state.edges))).toEqual(
    sortedEdges(room.edges()),
  );
}

function expectListsEqualFresh(state: ClientState, room: SimMailroom) {
  const mail = room.mail();
  expect(state.stuck.rows).toEqual(mail.stuck);
  expect(state.moving.rows).toEqual(mail.moving);
}

function seededRandom(seed: number): (n: number) => number {
  let state = seed >>> 0;
  return (n) => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return Math.floor((state / 4294967296) * n);
  };
}

/** One step of every mailroom operation that writes a delivery. */
function randomStep(
  room: SimMailroom,
  pick: (n: number) => number,
): MailFrame[] {
  const people = ["human", A, B, C];
  const agents = [A, B, C];
  const choose = (items: string[]): string | undefined =>
    items[pick(items.length)];
  const moveFrom = (
    from: DeliveryState,
    to: DeliveryState,
    reason: string | null = null,
  ): MailFrame[] => {
    const id = choose(room.ids(from));
    return id === undefined ? [] : [room.move(id, to, reason)];
  };
  switch (pick(11)) {
    case 0:
    case 1:
      return [room.accept(people[pick(4)]!, agents[pick(3)]!)];
    case 2:
      // A duplicate accept and a rejected transaction write no event.
      return [];
    case 3:
      return [room.accept(people[pick(4)]!, agents[pick(3)]!, "held")];
    case 4:
      return moveFrom("queued", "offered");
    case 5:
      // Start: metadata only, the same state.
      return moveFrom("offered", "offered");
    case 6:
      return moveFrom("offered", "consumed");
    case 7:
      return moveFrom("offered", "uncertain", "session ended");
    case 8:
      return moveFrom("uncertain", "offered");
    case 9:
      return moveFrom("held", "queued");
    default:
      return moveFrom("held", "dead", "dropped");
  }
}

describe("invariant: snapshot at N plus every later frame equals a fresh snapshot", () => {
  for (const seed of [1, 2, 3, 4, 5]) {
    it(`holds after every step, seed ${seed}`, () => {
      const pick = seededRandom(seed);
      const room = new SimMailroom();
      for (let i = 0; i < 20; i += 1) {
        randomStep(room, pick);
      }
      let state = fresh(room);
      for (let step = 0; step < 600; step += 1) {
        state = apply(state, ...randomStep(room, pick));
        expectEdgesEqualFresh(state, room);
        expectListsEqualFresh(state, room);
        expect(state.cursor).toBe(room.head());
      }
      expect(state.anchor).toEqual(room.anchor());
    });
  }

  it("gives the same result however frames are batched", () => {
    const pick = seededRandom(9);
    const room = new SimMailroom();
    const start = fresh(room);
    const frames: MailFrame[] = [];
    for (let step = 0; step < 300; step += 1) {
      frames.push(...randomStep(room, pick));
    }
    const oneByOne = frames.reduce((s, f) => apply(s, f), start);
    const atOnce = apply(start, ...frames);
    expect(atOnce.edges).toEqual(oneByOne.edges);
    expect(atOnce.stuck.rows).toEqual(oneByOne.stuck.rows);
    expect(atOnce.moving.rows).toEqual(oneByOne.moving.rows);
    expectEdgesEqualFresh(atOnce, room);
  });

  it("does not change the input state", () => {
    const room = new SimMailroom();
    room.accept("human", A);
    const before = fresh(room);
    const copy = structuredClone(before);
    apply(before, room.accept("human", A), room.move("m000001", "offered"));
    expect(before).toEqual(copy);
  });
});

describe("prev_state is what makes the edge counts exact", () => {
  it("diverges from a fresh snapshot when a client ignores prev_state", () => {
    const pick = seededRandom(1);
    const room = new SimMailroom();
    let exact = fresh(room);
    let ignoring = exact;
    for (let step = 0; step < 200; step += 1) {
      for (const frame of randomStep(room, pick)) {
        exact = apply(exact, frame);
        // The transition's source is withheld, so nothing is ever decremented.
        ignoring = apply(ignoring, { ...frame, prev_state: null });
      }
    }
    expectEdgesEqualFresh(exact, room);
    expect(() => expectEdgesEqualFresh(ignoring, room)).toThrow();
  });
});

describe("resume", () => {
  it("echoes the anchor verbatim", () => {
    const room = new SimMailroom();
    room.accept("human", A);
    const state = fresh(room);
    expect(eventsTarget(state)).toBe("/events?cursor=1&nonce=nonce-1");
    const odd = { ...state, anchor: { seq: 7, nonce: "a b+c/=&d" } };
    const url = new URL(`http://x${eventsTarget(odd)}`);
    expect(url.searchParams.get("cursor")).toBe("7");
    expect(url.searchParams.get("nonce")).toBe("a b+c/=&d");
  });

  it("sends no anchor before a snapshot, or when mail_events is empty", () => {
    expect(eventsTarget(initialState())).toBe("/events");
    expect(eventsTarget(fresh(new SimMailroom()))).toBe("/events");
  });

  it("takes the anchor from the last applied frame", () => {
    const room = new SimMailroom();
    const state = apply(fresh(room), room.accept("human", A));
    expect(state.anchor).toEqual({ seq: 1, nonce: "nonce-1" });
    expect(eventsTarget(state)).toBe("/events?cursor=1&nonce=nonce-1");
  });

  it("keeps its own anchor and mail state on a mail: null snapshot", () => {
    const room = new SimMailroom();
    room.accept("human", A);
    room.accept(A, B, "held");
    const state = fresh(room);
    const resumed = apply(
      reduce(reduce(state, { type: "disconnected" }), { type: "socket_open" }),
      room.nullSnapshot(state.cursor!),
    );
    expect(resumed.link).toBe("live");
    expect(resumed.anchor).toEqual(state.anchor);
    expect(resumed.cursor).toBe(state.cursor);
    expect(resumed.edges).toEqual(state.edges);
    expect(resumed.stuck.rows).toEqual(state.stuck.rows);
    expect(resumed.moving.rows).toEqual(state.moving.rows);
  });

  it("never jumps past frames it has not received", () => {
    const room = new SimMailroom();
    room.accept("human", A);
    const state = fresh(room);
    // Three more writes the client has not seen.
    const later = [
      room.accept("human", B),
      room.move("m000001", "offered"),
      room.move("m000001", "consumed"),
    ];
    // Even a mail: null snapshot naming the server's head does not move it.
    const resumed = apply(state, room.nullSnapshot(room.head()));
    expect(resumed.cursor).toBe(1);
    expect(eventsTarget(resumed)).toBe("/events?cursor=1&nonce=nonce-1");
    const caughtUp = apply(resumed, ...later);
    expect(caughtUp.cursor).toBe(room.head());
    expectEdgesEqualFresh(caughtUp, room);
    expectListsEqualFresh(caughtUp, room);
  });

  it("applies each seq once", () => {
    const room = new SimMailroom();
    const frame = room.accept("human", A);
    const once = apply(fresh(new SimMailroom()), frame);
    const twice = apply(once, frame);
    expect(twice.edges).toEqual(once.edges);
    expect(twice.moving.rows).toEqual(once.moving.rows);
  });

  it("replaces all mail state on a full snapshot, even at a lower cursor", () => {
    const room = new SimMailroom();
    for (let i = 0; i < 5; i += 1) {
      room.accept("human", A);
    }
    const state = fresh(room);
    const restored = new SimMailroom();
    restored.accept(B, C, "held");
    const next = apply(state, restored.snapshot());
    expect(next.cursor).toBe(1);
    expect(next.anchor).toEqual(restored.anchor());
    expectEdgesEqualFresh(next, restored);
    expectListsEqualFresh(next, restored);
  });

  it("clears the anchor when a full snapshot has none", () => {
    const room = new SimMailroom();
    room.accept("human", A);
    const state = fresh(room);
    const wiped = apply(state, new SimMailroom().snapshot());
    expect(wiped.anchor).toBeNull();
    expect(eventsTarget(wiped)).toBe("/events");
  });
});

/** Drives the reducer as `FeedClient` does, with a virtual clock and a real read. */
class Harness {
  state: ClientState;
  now = 1000;
  /** Reads taken when each request was sent, awaiting delivery. */
  inflight: {
    list: ListName;
    id: number;
    rows: ReturnType<SimMailroom["read"]>;
  }[] = [];
  requests = 0;

  constructor(
    readonly room: SimMailroom,
    state?: ClientState,
  ) {
    this.state = state ?? fresh(room);
    this.pump();
  }

  private pump() {
    const plan = planFetches(this.state, this.now);
    for (const { list, id } of plan.send) {
      this.state = reduce(this.state, {
        type: "fetch_sent",
        list,
        id,
        now: this.now,
      });
      this.requests += 1;
      this.inflight.push({
        list,
        id,
        rows: this.room.read(LIST_STATES[list]),
      });
    }
  }

  frames(...frames: Frame[]) {
    this.state = reduce(this.state, { type: "frames", frames, now: this.now });
    this.pump();
  }

  /** Deliver the oldest outstanding result. */
  deliver() {
    const request = this.inflight.shift();
    if (!request) {
      throw new Error("no request in flight");
    }
    this.state = reduce(this.state, {
      type: "fetch_ok",
      list: request.list,
      id: request.id,
      rows: request.rows,
    });
    this.pump();
  }

  fail() {
    const request = this.inflight.shift();
    if (!request) {
      throw new Error("no request in flight");
    }
    this.state = reduce(this.state, {
      type: "fetch_failed",
      list: request.list,
      id: request.id,
    });
    this.pump();
  }

  advance(ms: number) {
    this.now += ms;
    this.pump();
  }

  /** Writes have stopped: run until nothing is dirty or in flight. */
  settle() {
    for (let i = 0; i < 20; i += 1) {
      while (this.inflight.length > 0) {
        this.deliver();
      }
      this.advance(REFETCH_INTERVAL_MS);
      if (
        this.inflight.length === 0 &&
        !this.state.stuck.dirty &&
        !this.state.moving.dirty
      ) {
        return;
      }
    }
    throw new Error("did not settle");
  }
}

function heldRoom(count: number): SimMailroom {
  const room = new SimMailroom();
  for (let i = 0; i < count; i += 1) {
    room.accept("human", A, "held");
  }
  return room;
}

describe("feed mode", () => {
  it("is not changed by /mail results, and a resume after them is correct", () => {
    const room = heldRoom(3);
    const state = fresh(room);
    expect(state.stuck.mode).toBe("feed");
    const after = reduce(state, {
      type: "fetch_ok",
      list: "stuck",
      id: 1,
      rows: [],
    });
    expect(after.stuck.rows).toEqual(state.stuck.rows);
    expect(eventsTarget(after)).toBe(eventsTarget(state));
    const failed = reduce(state, {
      type: "fetch_failed",
      list: "stuck",
      id: 1,
    });
    expect(failed.stuck.dirty).toBe(false);
    const frame = room.move("m000001", "dead");
    const caughtUp = apply(after, room.nullSnapshot(after.cursor!), frame);
    expectListsEqualFresh(caughtUp, room);
    expectEdgesEqualFresh(caughtUp, room);
  });

  it("sends no request", () => {
    const harness = new Harness(heldRoom(3));
    harness.frames(harness.room.accept("human", B, "held"));
    harness.advance(5000);
    expect(harness.requests).toBe(0);
  });

  it("holds exactly the bound without entering view mode", () => {
    const state = fresh(heldRoom(LIST_BOUND));
    expect(state.stuck.mode).toBe("feed");
    expect(state.stuck.rows).toHaveLength(LIST_BOUND);
  });
});

describe("view mode", () => {
  it("is entered from a truncated snapshot, with the exact count on the edges", () => {
    const room = heldRoom(LIST_BOUND + 5);
    const harness = new Harness(room);
    expect(harness.state.stuck.mode).toBe("view");
    expect(harness.state.moving.mode).toBe("feed");
    expect(harness.state.stuck.rows).toHaveLength(LIST_BOUND);
    expect(harness.state.edges[edgeKey("human", A)]!.held).toBe(LIST_BOUND + 5);
    expect(harness.requests).toBe(1);
  });

  it("is entered when frames grow a list past the bound, even on the last write", () => {
    const room = heldRoom(LIST_BOUND);
    const harness = new Harness(room);
    expect(harness.requests).toBe(0);
    harness.frames(room.accept("human", A, "held"));
    expect(harness.state.stuck.mode).toBe("view");
    expect(harness.state.stuck.rows).toHaveLength(LIST_BOUND);
    expect(harness.requests).toBe(1);
    harness.settle();
    expect(harness.state.stuck.rows).toEqual(room.mail().stuck);
    expectEdgesEqualFresh(harness.state, room);
  });

  it("frames never edit its rows, and only relevant frames mark it dirty", () => {
    const room = heldRoom(LIST_BOUND + 1);
    const harness = new Harness(room);
    harness.deliver();
    const rows = harness.state.stuck.rows;
    harness.frames(room.accept("human", B));
    expect(harness.state.stuck.dirty).toBe(false);
    harness.frames(room.move("m000002", "dead"));
    expect(harness.state.stuck.rows).toEqual(rows);
    expect(harness.state.stuck.dirty).toBe(true);
    expectEdgesEqualFresh(harness.state, room);
  });

  it("marks dirty on a metadata-only change to a row in its states", () => {
    const room = heldRoom(LIST_BOUND + 1);
    const harness = new Harness(room);
    harness.deliver();
    harness.frames(room.move("m000002", "held", "new reason"));
    expect(harness.state.stuck.dirty).toBe(true);
  });

  it("allows one request per list at a time, and one per second", () => {
    const room = heldRoom(LIST_BOUND + 1);
    const harness = new Harness(room);
    expect(harness.requests).toBe(1);
    // A frame during the request cannot start a second one.
    harness.frames(room.move("m000002", "dead"));
    harness.advance(5000);
    expect(harness.requests).toBe(1);
    // The result landed, and the suppressed request goes as soon as the rate allows.
    harness.deliver();
    expect(harness.requests).toBe(2);
    harness.deliver();
    expect(harness.state.stuck.dirty).toBe(false);
    harness.frames(room.move("m000003", "dead"));
    expect(harness.requests).toBe(2);
    const plan = planFetches(harness.state, harness.now);
    expect(plan.send).toEqual([]);
    expect(plan.wakeAt).toBe(harness.now + REFETCH_INTERVAL_MS);
  });

  it("wakes at the earliest allowed time, so a suppressed attempt is not lost", () => {
    const room = heldRoom(LIST_BOUND + 1);
    const harness = new Harness(room);
    harness.deliver();
    harness.advance(100);
    harness.frames(room.move("m000002", "dead"));
    expect(harness.requests).toBe(1);
    const plan = planFetches(harness.state, harness.now);
    expect(plan.wakeAt).toBe(1000 + REFETCH_INTERVAL_MS);
    harness.advance(900);
    expect(harness.requests).toBe(2);
  });

  it("two overlapping fetches cannot both land", () => {
    const room = heldRoom(LIST_BOUND + 1);
    let state = fresh(room);
    state = reduce(state, {
      type: "fetch_sent",
      list: "stuck",
      id: 10,
      now: 0,
    });
    const rows = room.read(LIST_STATES.stuck);
    state = reduce(state, { type: "disconnected" });
    state = reduce(state, { type: "socket_open" });
    state = apply(state, room.nullSnapshot(state.cursor!));
    state = reduce(state, {
      type: "fetch_sent",
      list: "stuck",
      id: 11,
      now: 2000,
    });
    const stale = reduce(state, {
      type: "fetch_ok",
      list: "stuck",
      id: 10,
      rows: rows.slice(0, 1),
    });
    expect(stale.stuck.rows).toBe(state.stuck.rows);
    expect(stale.stuck.outstanding).toBe(11);
    const landed = reduce(stale, {
      type: "fetch_ok",
      list: "stuck",
      id: 11,
      rows,
    });
    expect(landed.stuck.rows).toEqual(rows);
    const again = reduce(landed, {
      type: "fetch_ok",
      list: "stuck",
      id: 11,
      rows: [],
    });
    expect(again.stuck.rows).toEqual(rows);
  });

  it("discards a result delayed across a reconnect", () => {
    const room = heldRoom(LIST_BOUND + 1);
    const harness = new Harness(room);
    const delayed = harness.inflight.shift()!;
    harness.state = reduce(harness.state, { type: "disconnected" });
    expect(harness.state.stuck.outstanding).toBeNull();
    const after = reduce(harness.state, {
      type: "fetch_ok",
      list: "stuck",
      id: delayed.id,
      rows: [],
    });
    expect(after.stuck.rows).toHaveLength(LIST_BOUND);
  });

  it("discards a result delayed across a retention-gap snapshot", () => {
    const room = heldRoom(LIST_BOUND + 1);
    const harness = new Harness(room);
    const delayed = harness.inflight.shift()!;
    // A full snapshot on the live connection, as after a retention gap.
    harness.state = apply(harness.state, room.snapshot());
    expect(harness.state.stuck.outstanding).toBeNull();
    expect(harness.state.stuck.dirty).toBe(true);
    const after = reduce(harness.state, {
      type: "fetch_ok",
      list: "stuck",
      id: delayed.id,
      rows: [],
    });
    expect(after.stuck.rows).toHaveLength(LIST_BOUND);
    // And a late failure of the cancelled request changes nothing either.
    const failed = reduce(
      { ...harness.state, stuck: { ...harness.state.stuck, dirty: false } },
      { type: "fetch_failed", list: "stuck", id: delayed.id },
    );
    expect(failed.stuck.dirty).toBe(false);
  });

  it("never asks for a continuation page", () => {
    for (const list of ["stuck", "moving"] as const) {
      expect(listTarget(list)).not.toContain("before_msg_seq");
    }
    expect(listTarget("stuck")).toBe(
      "/mail?states=held%2Cuncertain&limit=1000",
    );
    expect(listTarget("moving")).toBe(
      "/mail?states=queued%2Coffered&limit=1000",
    );
  });

  it("shows a row promoted from beyond the window after the refetch", () => {
    const room = heldRoom(LIST_BOUND + 5);
    const harness = new Harness(room);
    harness.deliver();
    const oldest = "m000001";
    expect(harness.state.stuck.rows.some((r) => r.id === oldest)).toBe(false);
    // Rows leave from the top, so the oldest enter the window.
    const frames: Frame[] = [];
    for (let i = 0; i < 5; i += 1) {
      frames.push(room.move(`m${String(1005 - i).padStart(6, "0")}`, "dead"));
    }
    harness.frames(...frames);
    harness.advance(REFETCH_INTERVAL_MS);
    harness.settle();
    expect(harness.state.stuck.rows.some((r) => r.id === oldest)).toBe(true);
    expect(harness.state.stuck.rows).toEqual(room.mail().stuck);
    expectEdgesEqualFresh(harness.state, room);
  });

  it("equals a fresh read once writes stop", () => {
    const pick = seededRandom(5);
    const room = heldRoom(LIST_BOUND + 50);
    const harness = new Harness(room);
    for (let step = 0; step < 400; step += 1) {
      harness.frames(...randomStep(room, pick));
      harness.advance(pick(40));
      if (pick(10) === 0 && harness.inflight.length > 0) {
        harness.deliver();
      }
      expectEdgesEqualFresh(harness.state, room);
    }
    harness.settle();
    expect(harness.state.stuck.rows).toEqual(room.mail().stuck);
    expect(harness.state.stuck.mode).toBe("view");
    expectEdgesEqualFresh(harness.state, room);
  });

  it("converges after a frame during a request", () => {
    const room = heldRoom(LIST_BOUND + 1);
    const harness = new Harness(room);
    // Read taken, then the final frame, then the response.
    harness.frames(room.move("m001001", "dead"));
    expect(harness.state.stuck.dirty).toBe(true);
    harness.deliver();
    // The result was read before the frame's write, so dirty stays set.
    expect(harness.state.stuck.dirty).toBe(true);
    harness.settle();
    expect(harness.state.stuck.rows).toEqual(room.mail().stuck);
  });

  it("converges after a frame inside the one-second window", () => {
    const room = heldRoom(LIST_BOUND + 1);
    const harness = new Harness(room);
    harness.deliver();
    harness.advance(10);
    harness.frames(room.move("m001001", "dead"));
    expect(harness.requests).toBe(1);
    harness.settle();
    expect(harness.requests).toBe(2);
    expect(harness.state.stuck.rows).toEqual(room.mail().stuck);
  });

  it("converges after a failed request", () => {
    const room = heldRoom(LIST_BOUND + 1);
    const harness = new Harness(room);
    room.move("m001001", "dead");
    harness.fail();
    expect(harness.state.stuck.dirty).toBe(true);
    expect(harness.state.stuck.outstanding).toBeNull();
    harness.settle();
    expect(harness.state.stuck.rows).toEqual(room.mail().stuck);
  });

  it("marks lists dirty on every connection, including a resume", () => {
    const room = heldRoom(LIST_BOUND + 1);
    const harness = new Harness(room);
    harness.deliver();
    expect(harness.state.stuck.dirty).toBe(false);
    harness.state = reduce(harness.state, { type: "disconnected" });
    harness.state = reduce(harness.state, { type: "socket_open" });
    expect(harness.state.stuck.dirty).toBe(true);
    expect(harness.state.moving.dirty).toBe(false);
  });

  it("sends no request while disconnected or before the snapshot", () => {
    const room = heldRoom(LIST_BOUND + 1);
    const harness = new Harness(room);
    harness.deliver();
    harness.state = reduce(harness.state, { type: "disconnected" });
    harness.frames();
    harness.state = { ...harness.state, link: "disconnected" };
    harness.state = {
      ...harness.state,
      stuck: { ...harness.state.stuck, dirty: true },
    };
    harness.advance(5000);
    expect(harness.requests).toBe(1);
    harness.state = reduce(harness.state, { type: "socket_open" });
    harness.advance(5000);
    expect(harness.requests).toBe(1);
    harness.frames(room.nullSnapshot(harness.state.cursor!));
    expect(harness.requests).toBe(2);
  });

  it("keeps the rate across a reconnect", () => {
    const room = heldRoom(LIST_BOUND + 1);
    const harness = new Harness(room);
    harness.state = reduce(harness.state, { type: "disconnected" });
    harness.inflight = [];
    harness.state = reduce(harness.state, { type: "socket_open" });
    harness.frames(room.nullSnapshot(harness.state.cursor!));
    expect(harness.requests).toBe(1);
    harness.advance(REFETCH_INTERVAL_MS);
    expect(harness.requests).toBe(2);
  });

  it("leaves only on a full snapshot with its flag unset", () => {
    const room = heldRoom(LIST_BOUND + 1);
    let harness = new Harness(room);
    harness.deliver();
    harness.frames(room.move("m001001", "dead"), room.move("m001000", "dead"));
    harness.settle();
    // Below the bound again, but only a full snapshot can restore feed mode.
    expect(harness.state.stuck.rows.length).toBeLessThan(LIST_BOUND);
    expect(harness.state.stuck.mode).toBe("view");
    harness.frames(room.nullSnapshot(harness.state.cursor!));
    expect(harness.state.stuck.mode).toBe("view");
    harness.frames(room.snapshot());
    expect(harness.state.stuck.mode).toBe("feed");
    expect(harness.state.stuck.dirty).toBe(false);
    harness = new Harness(room, harness.state);
    harness.frames(room.move("m000999", "dead"));
    expect(harness.requests).toBe(0);
    expectListsEqualFresh(harness.state, room);
  });

  it("keeps the two lists independent", () => {
    const room = heldRoom(LIST_BOUND + 1);
    const harness = new Harness(room);
    harness.deliver();
    const q = room.accept("human", B);
    harness.frames(q);
    expect(harness.state.moving.mode).toBe("feed");
    expect(harness.state.moving.rows.map((r) => r.id)).toEqual([q.id]);
    expect(harness.state.stuck.dirty).toBe(false);
  });
});

describe("volatile frames", () => {
  const agent = {
    id: A,
    name: "@a",
    kind: "claude",
    isolation: "none",
    phase: "idle" as const,
    error: null,
  };
  const request = {
    id: "p1",
    agent: A,
    incarnation: 1,
    tool: "edit",
    title: "t",
    detail: "",
    options: [],
    options_truncated: false,
  };

  it("keeps a known name when a frame missed its lookup", () => {
    let state = apply(initialState(), { event: "agent", ...agent });
    state = apply(state, {
      event: "agent",
      ...agent,
      name: "",
      kind: "",
      phase: "failed",
      error: "boom",
    });
    expect(state.agents[A]).toEqual({
      ...agent,
      phase: "failed",
      error: "boom",
    });
    expect(state.agents[A]).not.toHaveProperty("event");
  });

  it("tracks permissions, and ignores an answer for an unknown id", () => {
    let state = apply(initialState(), {
      event: "permission_pending",
      request,
    });
    expect(Object.keys(state.approvals)).toEqual(["p1"]);
    state = apply(state, {
      event: "permission_answered",
      agent: A,
      id: "nope",
    });
    expect(Object.keys(state.approvals)).toEqual(["p1"]);
    state = apply(state, { event: "permission_answered", agent: A, id: "p1" });
    expect(state.approvals).toEqual({});
  });

  it("replaces agents and approvals on any snapshot, without touching mail", () => {
    const room = new SimMailroom();
    room.accept("human", A);
    let state = apply(fresh(room), {
      event: "permission_pending",
      request,
    });
    state = apply(state, {
      ...room.nullSnapshot(state.cursor!),
      agents: [agent],
      approvals_truncated: true,
    });
    expect(state.approvals).toEqual({});
    expect(state.approvalsTruncated).toBe(true);
    expect(Object.keys(state.agents)).toEqual([A]);
    expectEdgesEqualFresh(state, room);
  });

  it("keeps a bounded activity tail, across snapshots", () => {
    const room = new SimMailroom();
    let state = fresh(room);
    for (let i = 0; i < 6; i += 1) {
      state = apply(state, {
        event: "activity",
        agent: A,
        text: `${i}`.repeat(2048),
      });
    }
    expect(state.activity[A]).toHaveLength(8192);
    expect(state.activity[A]!.endsWith("5")).toBe(true);
    state = apply(state, room.nullSnapshot(0));
    expect(state.activity[A]).toHaveLength(8192);
  });

  it("never starts a tail on half a surrogate pair", () => {
    const text = "😀".repeat(5000);
    const state = apply(initialState(), { event: "activity", agent: A, text });
    const first = state.activity[A]!.charCodeAt(0);
    expect(first >= 0xdc00 && first <= 0xdfff).toBe(false);
  });

  it("records outcomes and a bounded list of egress denials", () => {
    let state = apply(initialState(), {
      event: "turn_ended",
      agent: A,
      outcome: "failed",
    });
    expect(state.turns[A]).toBe("failed");
    for (let i = 0; i < 30; i += 1) {
      state = apply(state, {
        event: "egress_denied",
        agent: A,
        host: `h${i}`,
        port: 443,
      });
    }
    expect(state.egress[A]).toHaveLength(20);
    expect(state.egress[A]!.at(-1)!.host).toBe("h29");
  });

  it("ignores event kinds it does not know", () => {
    const state = initialState();
    const next = apply(state, { event: "future" } as unknown as Frame);
    expect(next.edges).toEqual(state.edges);
    expect(next.agents).toEqual(state.agents);
  });
});

describe("rejection", () => {
  it("stays rejected across a late disconnect, and cancels requests", () => {
    const room = heldRoom(LIST_BOUND + 1);
    const harness = new Harness(room);
    let state = reduce(harness.state, { type: "rejected" });
    expect(state.stuck.outstanding).toBeNull();
    state = reduce(state, { type: "disconnected" });
    expect(state.link).toBe("rejected");
    expect(planFetches(state, 1e9).send).toEqual([]);
  });
});
