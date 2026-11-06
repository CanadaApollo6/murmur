// The client's mail state, as one pure reducer. Section 3 of the milestone 5
// design is the specification. In short:
//
// - Edges, the anchor and the cursor only ever come from the feed.
// - `stuck` and `moving` are each in exactly one of two modes. In feed mode only
//   frames change the list, so it stays consistent with the anchor. In view mode
//   the list is a read-only copy of one `/mail` result, and frames only mark it
//   dirty. The two sources are never merged.
//
// Time is an input (`now` on the actions that need it), so the reducer can be
// tested without a browser or a clock.
import type {
  AgentRow,
  Anchor,
  DeliveryState,
  EdgeCounts,
  Frame,
  FullMail,
  MailFrame,
  MailRow,
  PermissionRequest,
  SnapshotFrame,
} from "./types";

export type ListName = "stuck" | "moving";

/** The size of the snapshot's lists, and of a `/mail` page. */
export const LIST_BOUND = 1000;
/** At most one request per list per this many milliseconds. */
export const REFETCH_INTERVAL_MS = 1000;
/** Activity arrives as newest-2-KiB chunks; this is how much tail a client keeps. */
export const ACTIVITY_TAIL_CHARS = 8192;
const EGRESS_KEPT = 20;

export const LIST_STATES: Record<ListName, readonly DeliveryState[]> = {
  stuck: ["held", "uncertain"],
  moving: ["queued", "offered"],
};
const LIST_NAMES: readonly ListName[] = ["stuck", "moving"];

export interface MailList {
  mode: "feed" | "view";
  rows: MailRow[];
  /** View mode only: frames arrived that the rows may not reflect. */
  dirty: boolean;
  /** The id of the one request whose result may land. */
  outstanding: number | null;
  /** When the last request was sent. Survives reconnects, so the rate does. */
  lastSentAt: number | null;
}

export type Link =
  /** No snapshot has arrived on the current socket yet. */
  | "connecting"
  | "live"
  | "disconnected"
  /** The token was refused. Only a new `agentctl ui` URL can recover. */
  | "rejected";

export interface EgressDenial {
  host: string;
  port: number;
  at: number;
}

export interface ClientState {
  link: Link;
  /** A full mail snapshot has been applied. */
  hasMail: boolean;
  /** The position after which frames apply. */
  cursor: number | null;
  /** Echoed verbatim on resume. Treated as opaque. */
  anchor: Anchor | null;
  agents: Record<string, AgentRow>;
  agentsTruncated: boolean;
  approvals: Record<string, PermissionRequest>;
  approvalsTruncated: boolean;
  edges: Record<string, EdgeCounts>;
  edgesTruncated: boolean;
  stuck: MailList;
  moving: MailList;
  activity: Record<string, string>;
  turns: Record<string, string>;
  egress: Record<string, EgressDenial[]>;
  /** Display only: when each edge last carried a frame. */
  edgeTouched: Record<string, number>;
  nextRequestId: number;
  /** When the feed last delivered anything. */
  lastFrameAt: number | null;
}

export type Action =
  | { type: "socket_open" }
  | { type: "frames"; frames: readonly Frame[]; now: number }
  | { type: "disconnected" }
  | { type: "rejected" }
  | { type: "fetch_sent"; list: ListName; id: number; now: number }
  | { type: "fetch_ok"; list: ListName; id: number; rows: MailRow[] }
  | { type: "fetch_failed"; list: ListName; id: number };

function emptyList(): MailList {
  return {
    mode: "feed",
    rows: [],
    dirty: false,
    outstanding: null,
    lastSentAt: null,
  };
}

export function initialState(): ClientState {
  return {
    link: "connecting",
    hasMail: false,
    cursor: null,
    anchor: null,
    agents: {},
    agentsTruncated: false,
    approvals: {},
    approvalsTruncated: false,
    edges: {},
    edgesTruncated: false,
    stuck: emptyList(),
    moving: emptyList(),
    activity: {},
    turns: {},
    egress: {},
    edgeTouched: {},
    nextRequestId: 1,
    lastFrameAt: null,
  };
}

export function edgeKey(from: string, to: string): string {
  return `${from}\n${to}`;
}

function edgesFrom(rows: readonly EdgeCounts[]): Record<string, EdgeCounts> {
  const edges: Record<string, EdgeCounts> = {};
  for (const edge of rows) {
    edges[edgeKey(edge.from, edge.to)] = edge;
  }
  return edges;
}

function newestFirst(rows: readonly MailRow[]): MailRow[] {
  return [...rows].sort((a, b) => b.msg_seq - a.msg_seq);
}

function listFromSnapshot(
  previous: MailList,
  rows: MailRow[],
  truncated: boolean,
): MailList {
  return {
    mode: truncated ? "view" : "feed",
    rows: newestFirst(rows),
    dirty: truncated,
    // A full snapshot cancels whatever was in flight.
    outstanding: null,
    lastSentAt: previous.lastSentAt,
  };
}

function applyFullMail(
  draft: ClientState,
  frame: SnapshotFrame,
  mail: FullMail,
) {
  draft.hasMail = true;
  draft.cursor = frame.mail_cursor;
  draft.anchor = mail.anchor;
  draft.edges = edgesFrom(mail.edges);
  draft.edgesTruncated = mail.edges_truncated;
  draft.edgeTouched = {};
  draft.stuck = listFromSnapshot(draft.stuck, mail.stuck, mail.stuck_truncated);
  draft.moving = listFromSnapshot(
    draft.moving,
    mail.moving,
    mail.moving_truncated,
  );
}

function applySnapshot(draft: ClientState, frame: SnapshotFrame) {
  draft.link = "live";
  draft.agents = {};
  for (const agent of frame.agents) {
    draft.agents[agent.id] = agent;
  }
  draft.agentsTruncated = frame.agents_truncated;
  draft.approvals = {};
  for (const request of frame.approvals) {
    draft.approvals[request.id] = request;
  }
  draft.approvalsTruncated = frame.approvals_truncated;
  // `mail: null` keeps the client's own anchor, cursor and mail state. Its
  // `mail_cursor` is deliberately not read: a client must never move past frames
  // it has not received.
  if (frame.mail) {
    applyFullMail(draft, frame, frame.mail);
  }
}

function applyEdge(draft: ClientState, frame: MailFrame, now: number) {
  const key = edgeKey(frame.from, frame.to);
  draft.edgeTouched[key] = now;
  if (frame.prev_state === frame.state) {
    return;
  }
  const edge: EdgeCounts = draft.edges[key]
    ? { ...draft.edges[key] }
    : {
        from: frame.from,
        to: frame.to,
        queued: 0,
        offered: 0,
        consumed: 0,
        uncertain: 0,
        held: 0,
        dead: 0,
      };
  if (frame.prev_state !== null) {
    // Only a truncated edge list can lack the count this decrements.
    edge[frame.prev_state] = Math.max(0, edge[frame.prev_state] - 1);
  }
  edge[frame.state] += 1;
  if (
    edge.queued +
      edge.offered +
      edge.consumed +
      edge.uncertain +
      edge.held +
      edge.dead ===
    0
  ) {
    // A fresh snapshot only reports pairs with a nonzero count.
    delete draft.edges[key];
  } else {
    draft.edges[key] = edge;
  }
}

function metadataRow(frame: MailFrame): MailRow {
  const {
    event: _event,
    seq: _seq,
    nonce: _nonce,
    prev_state: _prevState,
    at: _at,
    ...row
  } = frame;
  return row;
}

function applyToFeedList(
  list: MailList,
  states: readonly DeliveryState[],
  frame: MailFrame,
) {
  const index = list.rows.findIndex((row) => row.id === frame.id);
  if (index >= 0) {
    list.rows.splice(index, 1);
  }
  if (!states.includes(frame.state)) {
    return;
  }
  let at = 0;
  while (at < list.rows.length && list.rows[at]!.msg_seq > frame.msg_seq) {
    at += 1;
  }
  list.rows.splice(at, 0, metadataRow(frame));
  if (list.rows.length > LIST_BOUND) {
    // Past the bound, frames alone can no longer say which rows belong to the
    // newest 1,000 once rows start leaving. The list moves to view mode.
    list.mode = "view";
    list.dirty = true;
    list.rows.length = LIST_BOUND;
  }
}

function applyMail(draft: ClientState, frame: MailFrame, now: number) {
  // A client applies each `seq` once.
  if (draft.cursor !== null && frame.seq <= draft.cursor) {
    return;
  }
  draft.cursor = frame.seq;
  draft.anchor = { seq: frame.seq, nonce: frame.nonce };
  applyEdge(draft, frame, now);
  for (const name of LIST_NAMES) {
    const list = draft[name];
    const states = LIST_STATES[name];
    if (list.mode === "feed") {
      applyToFeedList(list, states, frame);
    } else if (
      states.includes(frame.state) ||
      (frame.prev_state !== null && states.includes(frame.prev_state))
    ) {
      list.dirty = true;
    }
  }
}

function applyAgent(draft: ClientState, frame: AgentRow) {
  const previous = draft.agents[frame.id];
  // A missed name lookup still sends the frame, with what it knows.
  draft.agents[frame.id] = {
    id: frame.id,
    name: frame.name || previous?.name || "",
    kind: frame.kind || previous?.kind || "",
    isolation: frame.isolation,
    phase: frame.phase,
    error: frame.error,
  };
}

function tailOf(text: string): string {
  if (text.length <= ACTIVITY_TAIL_CHARS) {
    return text;
  }
  const start = text.length - ACTIVITY_TAIL_CHARS;
  const first = text.charCodeAt(start);
  // Never start on the second half of a surrogate pair.
  return text.slice(first >= 0xdc00 && first <= 0xdfff ? start + 1 : start);
}

function applyFrames(
  state: ClientState,
  frames: readonly Frame[],
  now: number,
) {
  // One copy per batch, so a page of 256 frames does not copy 256 times. The
  // copies are private to this call, so the reducer stays pure.
  const draft: ClientState = {
    ...state,
    agents: { ...state.agents },
    approvals: { ...state.approvals },
    edges: { ...state.edges },
    stuck: { ...state.stuck, rows: [...state.stuck.rows] },
    moving: { ...state.moving, rows: [...state.moving.rows] },
    activity: { ...state.activity },
    turns: { ...state.turns },
    egress: { ...state.egress },
    edgeTouched: { ...state.edgeTouched },
    lastFrameAt: now,
  };
  for (const frame of frames) {
    switch (frame.event) {
      case "snapshot":
        applySnapshot(draft, frame);
        break;
      case "mail":
        applyMail(draft, frame, now);
        break;
      case "agent":
        applyAgent(draft, frame);
        break;
      case "permission_pending":
        draft.approvals[frame.request.id] = frame.request;
        break;
      case "permission_answered":
        // An answer for an unknown id is ignored by construction.
        delete draft.approvals[frame.id];
        break;
      case "turn_ended":
        draft.turns[frame.agent] = frame.outcome;
        break;
      case "egress_denied":
        draft.egress[frame.agent] = [
          ...(draft.egress[frame.agent] ?? []),
          { host: frame.host, port: frame.port, at: now },
        ].slice(-EGRESS_KEPT);
        break;
      case "activity":
        draft.activity[frame.agent] = tailOf(
          (draft.activity[frame.agent] ?? "") + frame.text,
        );
        break;
      default:
        // A newer server may send more event kinds. They change nothing here.
        break;
    }
  }
  return draft;
}

function withList(
  state: ClientState,
  name: ListName,
  list: MailList,
): ClientState {
  return name === "stuck"
    ? { ...state, stuck: list }
    : { ...state, moving: list };
}

function mapLists(
  state: ClientState,
  change: (list: MailList) => MailList,
): ClientState {
  return { ...state, stuck: change(state.stuck), moving: change(state.moving) };
}

export function reduce(state: ClientState, action: Action): ClientState {
  switch (action.type) {
    case "socket_open":
      // Every connection, including a resume, marks view-mode lists dirty.
      return mapLists({ ...state, link: "connecting" }, (list) =>
        list.mode === "view" ? { ...list, dirty: true } : list,
      );
    case "frames":
      return applyFrames(state, action.frames, action.now);
    case "disconnected":
      // A disconnect cancels outstanding requests. Their results no longer land.
      return mapLists(
        {
          ...state,
          link: state.link === "rejected" ? "rejected" : "disconnected",
        },
        (list) => ({ ...list, outstanding: null }),
      );
    case "rejected":
      return mapLists({ ...state, link: "rejected" }, (list) => ({
        ...list,
        outstanding: null,
      }));
    case "fetch_sent": {
      const list = state[action.list];
      return {
        ...withList(state, action.list, {
          ...list,
          // Dirty clears only here. A result never clears it, because that
          // result may have been read before a later frame's write.
          dirty: false,
          outstanding: action.id,
          lastSentAt: action.now,
        }),
        nextRequestId: Math.max(state.nextRequestId, action.id + 1),
      };
    }
    case "fetch_ok": {
      const list = state[action.list];
      if (list.outstanding !== action.id || list.mode !== "view") {
        return state;
      }
      return withList(state, action.list, {
        ...list,
        outstanding: null,
        rows: newestFirst(action.rows).slice(0, LIST_BOUND),
      });
    }
    case "fetch_failed": {
      const list = state[action.list];
      if (list.outstanding !== action.id) {
        return state;
      }
      return withList(state, action.list, {
        ...list,
        outstanding: null,
        dirty: true,
      });
    }
  }
}

export interface FetchPlan {
  /** Lists that may send a request right now. */
  send: { list: ListName; id: number }[];
  /** When to look again, for a request the rate limit has suppressed. */
  wakeAt: number | null;
}

/** What the driver must do for view-mode lists. Pure, like the reducer. */
export function planFetches(state: ClientState, now: number): FetchPlan {
  const plan: FetchPlan = { send: [], wakeAt: null };
  if (state.link !== "live") {
    return plan;
  }
  let id = state.nextRequestId;
  for (const name of LIST_NAMES) {
    const list = state[name];
    if (list.mode !== "view" || !list.dirty || list.outstanding !== null) {
      continue;
    }
    const earliest =
      list.lastSentAt === null ? now : list.lastSentAt + REFETCH_INTERVAL_MS;
    if (earliest <= now) {
      plan.send.push({ list: name, id });
      id += 1;
    } else if (plan.wakeAt === null || earliest < plan.wakeAt) {
      plan.wakeAt = earliest;
    }
  }
  return plan;
}

/** The `/events` target for the next connection. Echoes the anchor verbatim. */
export function eventsTarget(state: ClientState): string {
  if (!state.hasMail || state.anchor === null) {
    return "/events";
  }
  const query = new URLSearchParams({
    cursor: String(state.anchor.seq),
    nonce: state.anchor.nonce,
  });
  return `/events?${query.toString()}`;
}

/** The `/mail` target of a view-mode refetch. Never a continuation page. */
export function listTarget(name: ListName): string {
  const query = new URLSearchParams({
    states: LIST_STATES[name].join(","),
    limit: String(LIST_BOUND),
  });
  return `/mail?${query.toString()}`;
}
