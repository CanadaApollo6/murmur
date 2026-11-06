import { describe, expect, it } from "vitest";
import { initialState, reduce } from "../feed/reducer";
import { SimMailroom } from "../feed/simulator";
import type { AgentRow, SnapshotFrame } from "../feed/types";
import { HUMAN, buildGraph, displayName, edgesToDraw, shortId } from "./model";

function agent(
  id: string,
  phase: AgentRow["phase"],
  name = `@${id}`,
): AgentRow {
  return { id, name, kind: "claude", isolation: "none", phase, error: null };
}

function stateWith(room: SimMailroom, agents: AgentRow[]) {
  const frame: SnapshotFrame = { ...room.snapshot(), agents };
  return reduce(initialState(), { type: "frames", frames: [frame], now: 0 });
}

describe("buildGraph", () => {
  it("is empty with no agents and no mail", () => {
    const model = buildGraph(stateWith(new SimMailroom(), []));
    expect(model.nodes).toEqual([]);
    expect(model.edges).toEqual([]);
  });

  it("always includes the human once anything exists", () => {
    const model = buildGraph(
      stateWith(new SimMailroom(), [agent("a", "idle")]),
    );
    expect(model.nodes.map((n) => n.id).sort()).toEqual(["a", HUMAN].sort());
  });

  it("draws agents that only appear on edges", () => {
    const room = new SimMailroom();
    room.accept("agent:gone", "agent:alsogone");
    const model = buildGraph(stateWith(room, []));
    const byId = new Map(model.nodes.map((n) => [n.id, n]));
    expect(byId.get("agent:gone")!.phase).toBe("unknown");
    expect(byId.get("agent:alsogone")!.inbound).toBe(1);
  });

  it("derives edge status with stuck over live over dead", () => {
    const room = new SimMailroom();
    room.accept("human", "a");
    const consumed = room.accept("a", "b");
    room.move(consumed.id, "consumed");
    const dead = room.accept("b", "c");
    room.move(dead.id, "dead");
    room.accept("c", "a");
    room.accept("c", "a", "held");
    const model = buildGraph(stateWith(room, []));
    const status = new Map(
      model.edges.map((e) => [`${e.from}>${e.to}`, e.status]),
    );
    expect(status.get("human>a")).toBe("live");
    expect(status.get("a>b")).toBe("calm");
    expect(status.get("b>c")).toBe("dead");
    expect(status.get("c>a")).toBe("stuck");
  });

  it("totals come from the exact edge counts, not the bounded lists", () => {
    const room = new SimMailroom();
    for (let i = 0; i < 1005; i += 1) {
      room.accept("human", "a", "held");
    }
    const state = stateWith(room, [agent("a", "idle")]);
    expect(state.stuck.rows).toHaveLength(1000);
    expect(buildGraph(state).totals.held).toBe(1005);
  });

  it("counts failed, needs-recovery and permissions", () => {
    const state = stateWith(new SimMailroom(), [
      agent("a", "failed"),
      agent("b", "needs-recovery"),
      agent("c", "crashed"),
    ]);
    const withPermission = reduce(state, {
      type: "frames",
      now: 0,
      frames: [
        {
          event: "permission_pending",
          request: {
            id: "p",
            agent: "a",
            incarnation: 1,
            tool: "t",
            title: "t",
            detail: "",
            options: [],
            options_truncated: false,
          },
        },
      ],
    });
    const { totals, nodes } = buildGraph(withPermission);
    expect(totals.failed).toBe(1);
    expect(totals.needsRecovery).toBe(1);
    expect(totals.crashed).toBe(1);
    expect(totals.permissions).toBe(1);
    expect(nodes.find((n) => n.id === "a")!.permissions).toBe(1);
  });

  it("has a stable node order, whatever the snapshot order", () => {
    const a = buildGraph(
      stateWith(new SimMailroom(), [agent("b", "idle"), agent("a", "idle")]),
    );
    const b = buildGraph(
      stateWith(new SimMailroom(), [agent("a", "idle"), agent("b", "idle")]),
    );
    expect(a.nodes.map((n) => n.id)).toEqual(b.nodes.map((n) => n.id));
  });
});

describe("names", () => {
  it("falls back to a short id", () => {
    const state = stateWith(new SimMailroom(), [
      agent("agent:01ABCDEFGHIJKLMNOPQRSTUVWX", "idle", ""),
    ]);
    expect(displayName(state, "agent:01ABCDEFGHIJKLMNOPQRSTUVWX")).toBe(
      "…QRSTUVWX",
    );
    expect(displayName(state, HUMAN)).toBe("human");
    expect(shortId("abc")).toBe("abc");
  });
});

describe("edgesToDraw", () => {
  it("keeps stuck, then live, then the busiest, and the selected node's edges", () => {
    const room = new SimMailroom();
    room.accept("human", "stuck", "held");
    room.accept("human", "live");
    for (let i = 0; i < 3; i += 1) {
      const frame = room.accept("human", "busy");
      room.move(frame.id, "consumed");
    }
    const frame = room.accept("human", "quiet");
    room.move(frame.id, "consumed");
    const { edges } = buildGraph(stateWith(room, []));
    expect(edgesToDraw(edges, 3, null).map((e) => e.to)).toEqual([
      "stuck",
      "live",
      "busy",
    ]);
    expect(edgesToDraw(edges, 1, "quiet").map((e) => e.to)).toEqual(["quiet"]);
    expect(edgesToDraw(edges, 10, null)).toHaveLength(4);
  });
});
