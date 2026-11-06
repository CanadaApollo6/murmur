import { describe, expect, it } from "vitest";
import { SimMailroom } from "./simulator";
import { parseFrame } from "./wire";

function parse(value: unknown) {
  return parseFrame(JSON.stringify(value));
}

const agent = {
  id: "agent:A",
  name: "@a",
  kind: "claude",
  isolation: "none",
  phase: "idle",
  incarnation: 1,
  error: null,
};

const request = {
  id: "p1",
  agent: "agent:A",
  incarnation: 1,
  tool: "edit",
  title: "t",
  detail: "",
  options: [{ key: "yes", label: "Allow", effect: "AllowOnce", command: null }],
  options_truncated: false,
};

const fullMail = {
  anchor: null,
  edges: [
    {
      from: "human",
      to: "agent:A",
      queued: 0,
      offered: 0,
      consumed: 0,
      uncertain: 0,
      held: 0,
      dead: 0,
    },
  ],
  edges_truncated: false,
  stuck: [],
  stuck_truncated: false,
  moving: [],
  moving_truncated: false,
};
const snapshot = {
  event: "snapshot",
  mail_cursor: 0,
  agents: [agent],
  agents_truncated: false,
  approvals: [request],
  approvals_truncated: false,
  mail: fullMail,
};
const row = {
  msg_seq: 1,
  id: "m",
  from: "human",
  to: "agent:A",
  hop: 0,
  act: "request",
  state: "queued",
  reason: null,
};
const mail = {
  ...row,
  event: "mail",
  seq: 1,
  nonce: "n",
  prev_state: null,
};

describe("the types of numbers and flags", () => {
  it("accepts the frames these are built from", () => {
    for (const frame of [snapshot, mail, { event: "agent", ...agent }]) {
      expect(parse(frame).kind).toBe("frame");
    }
    expect(
      parse({
        ...snapshot,
        mail: { ...fullMail, anchor: { seq: 0, nonce: "n" } },
      }).kind,
    ).toBe("frame");
  });

  it.each([
    ["a seq at the top of i64", { ...mail, seq: 9223372036854775807 }],
    [
      "a cursor at the top of i64",
      { ...snapshot, mail_cursor: 9223372036854775807 },
    ],
    [
      "an incarnation at the top of u64",
      { event: "agent", ...agent, incarnation: 18446744073709551615 },
    ],
    ["an agent with no actor", { event: "agent", ...agent, incarnation: null }],
    ["a resume's snapshot", { ...snapshot, mail: null }],
  ])("accepts %s, which the server can send", (_name, frame) => {
    expect(parse(frame).kind).toBe("frame");
  });

  const withEdge = (change: Record<string, unknown>) => ({
    ...snapshot,
    mail: { ...fullMail, edges: [{ ...fullMail.edges[0], ...change }] },
  });
  const withMail = (change: Record<string, unknown>) => ({
    ...snapshot,
    mail: { ...fullMail, ...change },
  });
  it.each([
    ["agents_truncated", { ...snapshot, agents_truncated: "false" }],
    ["agents_truncated missing", { ...snapshot, agents_truncated: undefined }],
    ["approvals_truncated", { ...snapshot, approvals_truncated: 1 }],
    ["edges_truncated", withMail({ edges_truncated: "true" })],
    ["stuck_truncated", withMail({ stuck_truncated: 0 })],
    ["moving_truncated", withMail({ moving_truncated: null })],
    [
      "options_truncated",
      {
        event: "permission_pending",
        request: { ...request, options_truncated: "no" },
      },
    ],
    ["a fractional cursor", { ...snapshot, mail_cursor: 1.5 }],
    ["a string cursor", { ...snapshot, mail_cursor: "1" }],
    ["a fractional anchor", withMail({ anchor: { seq: 1.5, nonce: "n" } })],
    ["a fractional seq", { ...mail, seq: 1.5 }],
    ["a fractional msg_seq", { ...mail, msg_seq: 1.5 }],
    ["a fractional count", withEdge({ queued: 0.5 })],
    ["a negative count", withEdge({ held: -1 })],
    ["a fractional hop", { ...mail, hop: 0.5 }],
    [
      "a fractional port",
      { event: "egress_denied", agent: "a", host: "h", port: 1.5 },
    ],
    [
      "a fractional incarnation",
      { event: "agent", ...agent, incarnation: 1.5 },
    ],
    ["a string incarnation", { event: "agent", ...agent, incarnation: "1" }],
    ["no incarnation", { event: "agent", ...agent, incarnation: undefined }],
  ])("rejects %s", (_name, frame) => {
    expect(parse(frame).kind).toBe("invalid");
  });
});

describe("parseFrame", () => {
  it("accepts every frame the server sends", () => {
    const room = new SimMailroom();
    room.accept("human", "agent:A", "held");
    const frames = [
      room.snapshot(),
      room.nullSnapshot(0),
      room.accept("human", "agent:A"),
      { event: "agent", ...agent },
      { event: "permission_pending", request },
      { event: "permission_answered", agent: "agent:A", id: "p1" },
      { event: "turn_ended", agent: "agent:A", outcome: "completed" },
      { event: "egress_denied", agent: "agent:A", host: "h", port: 443 },
      { event: "activity", agent: "agent:A", text: "x" },
    ];
    for (const frame of frames) {
      expect(parse(frame)).toEqual({ kind: "frame", frame });
    }
  });

  it("calls an event kind it does not know unknown, not invalid", () => {
    expect(parse({ event: "from_the_future" })).toEqual({ kind: "unknown" });
    // Not fooled by a name every object inherits.
    expect(parse({ event: "constructor" })).toEqual({ kind: "unknown" });
  });

  it.each([
    ["no fields", { event: "snapshot" }],
    [
      "agents that are not a list",
      {
        event: "snapshot",
        mail_cursor: 0,
        agents: 1,
        approvals: [],
        mail: null,
      },
    ],
    [
      "an agent with no id",
      {
        event: "snapshot",
        mail_cursor: 0,
        agents: [{}],
        approvals: [],
        mail: null,
      },
    ],
    [
      "mail with no edges",
      {
        event: "snapshot",
        mail_cursor: 0,
        agents: [],
        approvals: [],
        mail: {},
      },
    ],
    ["a mail frame with no seq", { event: "mail", id: "m" }],
    [
      "a permission title that is an object",
      { event: "permission_pending", request: { ...request, title: {} } },
    ],
    [
      "a command that is a number",
      {
        event: "permission_pending",
        request: {
          ...request,
          options: [{ key: "k", label: "l", effect: "e", command: 1 }],
        },
      },
    ],
    ["activity with no text", { event: "activity", agent: "a" }],
  ])("rejects %s", (_name, frame) => {
    expect(parse(frame).kind).toBe("invalid");
  });

  it("rejects a mail frame with a state it does not know", () => {
    const room = new SimMailroom();
    const frame = { ...room.accept("human", "agent:A"), state: "lost" };
    expect(parse(frame).kind).toBe("invalid");
  });

  it("rejects what is not an object with an event name", () => {
    expect(parseFrame("{not json").kind).toBe("invalid");
    expect(parseFrame(new ArrayBuffer(1)).kind).toBe("invalid");
    expect(parse([1]).kind).toBe("invalid");
    expect(parse({ event: 1 }).kind).toBe("invalid");
    expect(parse(null).kind).toBe("invalid");
  });
});
