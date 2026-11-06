// The check at the feed's edge. A frame that reaches the reducer has the fields
// the reducer and the panels read, with the right types. This does not check that
// the values make sense: it stops a malformed frame from throwing later, inside a
// reducer or a render, where it would take the page down with it.
import {
  DELIVERY_STATES,
  type DeliveryState,
  type Frame,
  type MailRow,
} from "./types";

export type Parsed =
  | { kind: "frame"; frame: Frame }
  /** A kind from a newer server. It changes nothing here. */
  | { kind: "unknown" }
  | { kind: "invalid"; reason: string };

type Fields = Record<string, unknown>;

function isFields(value: unknown): value is Fields {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function isInteger(value: unknown): value is number {
  return Number.isInteger(value);
}

function isCount(value: unknown): boolean {
  return isInteger(value) && value >= 0;
}

function isBoolean(value: unknown): value is boolean {
  return typeof value === "boolean";
}

function isStringOrNull(value: unknown): boolean {
  return value === null || isString(value);
}

function isState(value: unknown): value is DeliveryState {
  return (DELIVERY_STATES as readonly unknown[]).includes(value);
}

function isArrayOf(value: unknown, item: (entry: unknown) => boolean): boolean {
  return Array.isArray(value) && value.every(item);
}

function isCommands(value: unknown): boolean {
  return (
    value === undefined ||
    (isFields(value) &&
      isStringOrNull(value.release) &&
      isStringOrNull(value.drop))
  );
}

export function isMailRow(value: unknown): value is MailRow {
  return (
    isFields(value) &&
    isInteger(value.msg_seq) &&
    isString(value.id) &&
    isString(value.from) &&
    isString(value.to) &&
    isCount(value.hop) &&
    isString(value.act) &&
    isState(value.state) &&
    isStringOrNull(value.reason) &&
    isCommands(value.commands)
  );
}

function isAgent(value: unknown): boolean {
  return (
    isFields(value) &&
    isString(value.id) &&
    isString(value.name) &&
    isString(value.kind) &&
    isString(value.isolation) &&
    isString(value.phase) &&
    (value.incarnation === null || isCount(value.incarnation)) &&
    isStringOrNull(value.error)
  );
}

function isOption(value: unknown): boolean {
  return (
    isFields(value) &&
    isString(value.key) &&
    isString(value.label) &&
    isString(value.effect) &&
    isStringOrNull(value.command)
  );
}

function isPermission(value: unknown): boolean {
  return (
    isFields(value) &&
    isString(value.id) &&
    isString(value.agent) &&
    isString(value.tool) &&
    isString(value.title) &&
    isString(value.detail) &&
    isArrayOf(value.options, isOption) &&
    isBoolean(value.options_truncated)
  );
}

function isEdge(value: unknown): boolean {
  return (
    isFields(value) &&
    isString(value.from) &&
    isString(value.to) &&
    DELIVERY_STATES.every((state) => isCount(value[state]))
  );
}

function isAnchor(value: unknown): boolean {
  return (
    value === null ||
    (isFields(value) && isInteger(value.seq) && isString(value.nonce))
  );
}

function isFullMail(value: unknown): boolean {
  return (
    isFields(value) &&
    isAnchor(value.anchor) &&
    isArrayOf(value.edges, isEdge) &&
    isBoolean(value.edges_truncated) &&
    isArrayOf(value.stuck, isMailRow) &&
    isBoolean(value.stuck_truncated) &&
    isArrayOf(value.moving, isMailRow) &&
    isBoolean(value.moving_truncated)
  );
}

const SHAPES: Record<Frame["event"], (frame: Fields) => boolean> = {
  snapshot: (frame) =>
    isInteger(frame.mail_cursor) &&
    isArrayOf(frame.agents, isAgent) &&
    isBoolean(frame.agents_truncated) &&
    isArrayOf(frame.approvals, isPermission) &&
    isBoolean(frame.approvals_truncated) &&
    (frame.mail === null || isFullMail(frame.mail)),
  mail: (frame) =>
    isMailRow(frame) &&
    isInteger(frame.seq) &&
    isString(frame.nonce) &&
    (frame.prev_state === null || isState(frame.prev_state)),
  agent: isAgent,
  permission_pending: (frame) => isPermission(frame.request),
  permission_answered: (frame) => isString(frame.id),
  turn_ended: (frame) => isString(frame.agent) && isString(frame.outcome),
  egress_denied: (frame) =>
    isString(frame.agent) && isString(frame.host) && isCount(frame.port),
  activity: (frame) => isString(frame.agent) && isString(frame.text),
};

function isKnownEvent(event: string): event is Frame["event"] {
  return Object.hasOwn(SHAPES, event);
}

export function parseFrame(data: unknown): Parsed {
  if (!isString(data)) {
    return { kind: "invalid", reason: "a binary message" };
  }
  let value: unknown;
  try {
    value = JSON.parse(data);
  } catch {
    return { kind: "invalid", reason: "a message that is not JSON" };
  }
  if (!isFields(value) || !isString(value.event)) {
    return { kind: "invalid", reason: "a message with no event name" };
  }
  if (!isKnownEvent(value.event)) {
    return { kind: "unknown" };
  }
  if (!SHAPES[value.event](value)) {
    return { kind: "invalid", reason: `a malformed ${value.event} frame` };
  }
  return { kind: "frame", frame: value as unknown as Frame };
}
