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

function isNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
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
    isNumber(value.msg_seq) &&
    isString(value.id) &&
    isString(value.from) &&
    isString(value.to) &&
    isNumber(value.hop) &&
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
    isArrayOf(value.options, isOption)
  );
}

function isEdge(value: unknown): boolean {
  return (
    isFields(value) &&
    isString(value.from) &&
    isString(value.to) &&
    DELIVERY_STATES.every((state) => isNumber(value[state]))
  );
}

function isAnchor(value: unknown): boolean {
  return (
    value === null ||
    (isFields(value) && isNumber(value.seq) && isString(value.nonce))
  );
}

function isFullMail(value: unknown): boolean {
  return (
    isFields(value) &&
    isAnchor(value.anchor) &&
    isArrayOf(value.edges, isEdge) &&
    isArrayOf(value.stuck, isMailRow) &&
    isArrayOf(value.moving, isMailRow)
  );
}

const SHAPES: Record<Frame["event"], (frame: Fields) => boolean> = {
  snapshot: (frame) =>
    isNumber(frame.mail_cursor) &&
    isArrayOf(frame.agents, isAgent) &&
    isArrayOf(frame.approvals, isPermission) &&
    (frame.mail === null || isFullMail(frame.mail)),
  mail: (frame) =>
    isMailRow(frame) &&
    isNumber(frame.seq) &&
    isString(frame.nonce) &&
    (frame.prev_state === null || isState(frame.prev_state)),
  agent: isAgent,
  permission_pending: (frame) => isPermission(frame.request),
  permission_answered: (frame) => isString(frame.id),
  turn_ended: (frame) => isString(frame.agent) && isString(frame.outcome),
  egress_denied: (frame) =>
    isString(frame.agent) && isString(frame.host) && isNumber(frame.port),
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
