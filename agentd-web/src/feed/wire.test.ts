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
