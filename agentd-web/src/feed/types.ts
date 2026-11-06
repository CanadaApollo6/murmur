// The wire shapes of `crates/agentd/src/surfaces/events.rs`. Every string in
// here that is not an id or a fixed vocabulary was written by an agent or an
// ACP server, and is hostile: it may only ever be rendered as plain text.

export const DELIVERY_STATES = [
  "queued",
  "offered",
  "consumed",
  "uncertain",
  "held",
  "dead",
] as const;
export type DeliveryState = (typeof DELIVERY_STATES)[number];

export interface Anchor {
  seq: number;
  nonce: string;
}

export interface HeldCommands {
  release: string | null;
  drop: string | null;
}

export interface MailRow {
  msg_seq: number;
  id: string;
  from: string;
  to: string;
  conversation: string;
  in_reply_to: string | null;
  hop: number;
  act: string;
  task: string | null;
  exposure: { sender: string; epoch: number; seq: number } | null;
  state: DeliveryState;
  turn_started: boolean;
  incarnation: number | null;
  attempts: number;
  reason: string | null;
  /** Present only on held rows. */
  commands?: HeldCommands;
}

export interface MailFrame extends MailRow {
  event: "mail";
  seq: number;
  nonce: string;
  prev_state: DeliveryState | null;
  at: string;
}

export type EdgeCounts = { from: string; to: string } & Record<
  DeliveryState,
  number
>;

export type Phase =
  | "dormant"
  | "starting"
  | "idle"
  | "busy"
  | "suspending"
  | "ended"
  | "crashed"
  | "failed"
  | "needs-recovery";

export interface AgentRow {
  id: string;
  name: string;
  kind: string;
  isolation: string;
  phase: Phase;
  /** Which life of this id. Null when a snapshot lists an agent with no actor. */
  incarnation: number | null;
  error: string | null;
}

export interface PermissionOption {
  key: string;
  label: string;
  effect: string;
  command: string | null;
}

export interface PermissionRequest {
  id: string;
  agent: string;
  incarnation: number;
  tool: string;
  title: string;
  detail: string;
  options: PermissionOption[];
  options_truncated: boolean;
}

export interface FullMail {
  anchor: Anchor | null;
  edges: EdgeCounts[];
  edges_truncated: boolean;
  stuck: MailRow[];
  moving: MailRow[];
  stuck_truncated: boolean;
  moving_truncated: boolean;
}

export interface SnapshotFrame {
  event: "snapshot";
  mail_cursor: number;
  agents: AgentRow[];
  agents_truncated: boolean;
  approvals: PermissionRequest[];
  approvals_truncated: boolean;
  mail: FullMail | null;
}

export type Frame =
  | SnapshotFrame
  | MailFrame
  | ({ event: "agent" } & AgentRow)
  | { event: "permission_pending"; request: PermissionRequest }
  | { event: "permission_answered"; agent: string; id: string }
  | { event: "turn_ended"; agent: string; outcome: string }
  | { event: "egress_denied"; agent: string; host: string; port: number }
  | { event: "activity"; agent: string; text: string };

export interface MailPage {
  rows: MailRow[];
  next_before_msg_seq: number | null;
}

/** `GET /messages/{id}`. Only the fields the UI reads. */
export interface MessageBody {
  body: string;
  bytes: number;
  sent_at: unknown;
}
