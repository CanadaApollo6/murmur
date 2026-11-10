// Derives what the graph and panels draw from the client state. Pure.
import { type ClientState, edgeKey } from "../feed/reducer";
import type { EdgeCounts, Phase } from "../feed/types";

export const HUMAN = "human";

export type NodeKind = Phase | "human" | "unknown";

export interface GraphNode {
  id: string;
  label: string;
  phase: NodeKind;
  kind: string;
  isolation: string;
  error: string | null;
  /** Pending permissions: waiting on the operator. */
  permissions: number;
  held: number;
  uncertain: number;
  /** Queued or offered mail addressed to this node. */
  inbound: number;
}

export type EdgeStatus = "stuck" | "live" | "dead" | "calm";

export interface GraphEdge {
  key: string;
  counts: EdgeCounts;
  from: string;
  to: string;
  status: EdgeStatus;
  total: number;
  stuck: number;
  live: number;
}

export interface Totals {
  agents: number;
  failed: number;
  needsRecovery: number;
  crashed: number;
  busy: number;
  permissions: number;
  held: number;
  uncertain: number;
  queued: number;
  offered: number;
  consumed: number;
  dead: number;
}

export interface GraphModel {
  nodes: GraphNode[];
  edges: GraphEdge[];
  totals: Totals;
}

/** The order an operator should look at phases in. Lower comes first. */
const PHASE_RANK: Record<NodeKind, number> = {
  failed: 0,
  "needs-recovery": 1,
  crashed: 2,
  busy: 3,
  starting: 4,
  suspending: 5,
  idle: 6,
  dormant: 7,
  stopped: 8,
  ended: 9,
  unknown: 10,
  human: 11,
};

export function phaseRank(phase: NodeKind): number {
  return PHASE_RANK[phase];
}

export function isBroken(phase: NodeKind): boolean {
  return phase === "failed" || phase === "needs-recovery";
}

export const PHASE_LABEL: Record<NodeKind, string> = {
  failed: "failed",
  "needs-recovery": "needs recovery",
  crashed: "crashed, retrying",
  busy: "busy",
  starting: "starting",
  suspending: "suspending",
  idle: "idle",
  dormant: "dormant",
  stopped: "stopped by operator",
  ended: "ended",
  unknown: "not listed",
  human: "you",
};

export function shortId(id: string): string {
  const bare = id.startsWith("agent:") ? id.slice("agent:".length) : id;
  return bare.length > 8 ? `…${bare.slice(-8)}` : bare;
}

export function displayName(state: ClientState, id: string): string {
  if (id === HUMAN) {
    return "human";
  }
  const name = state.agents[id]?.name;
  return name ? name : shortId(id);
}

function edgeFrom(counts: EdgeCounts): GraphEdge {
  const stuck = counts.held + counts.uncertain;
  const live = counts.queued + counts.offered;
  let status: EdgeStatus = "calm";
  if (stuck > 0) {
    status = "stuck";
  } else if (live > 0) {
    status = "live";
  } else if (counts.dead > 0) {
    status = "dead";
  }
  return {
    key: edgeKey(counts.from, counts.to),
    counts,
    from: counts.from,
    to: counts.to,
    status,
    total: stuck + live + counts.consumed + counts.dead,
    stuck,
    live,
  };
}

export function buildGraph(state: ClientState): GraphModel {
  const totals: Totals = {
    agents: 0,
    failed: 0,
    needsRecovery: 0,
    crashed: 0,
    busy: 0,
    permissions: 0,
    held: 0,
    uncertain: 0,
    queued: 0,
    offered: 0,
    consumed: 0,
    dead: 0,
  };
  const nodes = new Map<string, GraphNode>();
  const blank = (id: string, label: string, phase: NodeKind): GraphNode => ({
    id,
    label,
    phase,
    kind: "",
    isolation: "",
    error: null,
    permissions: 0,
    held: 0,
    uncertain: 0,
    inbound: 0,
  });
  for (const agent of Object.values(state.agents)) {
    totals.agents += 1;
    if (agent.phase === "failed") totals.failed += 1;
    if (agent.phase === "needs-recovery") totals.needsRecovery += 1;
    if (agent.phase === "crashed") totals.crashed += 1;
    if (agent.phase === "busy") totals.busy += 1;
    nodes.set(agent.id, {
      ...blank(agent.id, agent.name || shortId(agent.id), agent.phase),
      kind: agent.kind,
      isolation: agent.isolation,
      error: agent.error,
    });
  }
  const edges: GraphEdge[] = [];
  const node = (id: string): GraphNode => {
    let found = nodes.get(id);
    if (!found) {
      // A retired agent, or one beyond the snapshot's agent bound, still has
      // edges. Its traffic is real, so it is drawn.
      found = blank(id, shortId(id), id === HUMAN ? "human" : "unknown");
      nodes.set(id, found);
    }
    return found;
  };
  for (const counts of Object.values(state.edges)) {
    const edge = edgeFrom(counts);
    edges.push(edge);
    const to = node(counts.to);
    node(counts.from);
    to.held += counts.held;
    to.uncertain += counts.uncertain;
    to.inbound += edge.live;
    totals.held += counts.held;
    totals.uncertain += counts.uncertain;
    totals.queued += counts.queued;
    totals.offered += counts.offered;
    totals.consumed += counts.consumed;
    totals.dead += counts.dead;
  }
  for (const request of Object.values(state.approvals)) {
    totals.permissions += 1;
    node(request.agent).permissions += 1;
  }
  if (nodes.size > 0) {
    node(HUMAN);
  }
  const ordered = [...nodes.values()].sort((a, b) =>
    a.id < b.id ? -1 : a.id > b.id ? 1 : 0,
  );
  return { nodes: ordered, edges, totals };
}

/** At most `limit` edges to draw: the ones that matter, then the busiest. */
export function edgesToDraw(
  edges: readonly GraphEdge[],
  limit: number,
  keep: string | null,
): GraphEdge[] {
  if (edges.length <= limit) {
    return [...edges];
  }
  const weight = (edge: GraphEdge) =>
    (edge.from === keep || edge.to === keep ? 1e15 : 0) +
    (edge.status === "stuck" ? 1e14 : 0) +
    (edge.status === "live" ? 1e13 : 0) +
    edge.total;
  return [...edges].sort((a, b) => weight(b) - weight(a)).slice(0, limit);
}
