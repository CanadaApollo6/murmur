// A test-only model of the mailroom's triggers. It produces what the server sends
// for a sequence of operations: one frame per delivery change, and a fresh
// snapshot from current state. Reducer tests compare against the fresh snapshot.
import type {
  Anchor,
  DeliveryState,
  EdgeCounts,
  FullMail,
  HeldCommands,
  MailFrame,
  MailRow,
  SnapshotFrame,
} from "./types";

function commands(id: string): HeldCommands {
  return {
    release: `agentctl --socket '/s' release -- '${id}'`,
    drop: `agentctl --socket '/s' drop -- '${id}'`,
  };
}

export class SimMailroom {
  private rows = new Map<string, MailRow>();
  private nextMessage = 1;
  private seq = 0;
  private lastNonce: string | null = null;
  readonly log: MailFrame[] = [];

  private row(id: string): MailRow {
    const row = this.rows.get(id);
    if (!row) {
      throw new Error(`no message ${id}`);
    }
    return row;
  }

  private emit(
    row: MailRow,
    prev: DeliveryState | null,
    state: DeliveryState,
    reason: string | null,
    extra: Partial<MailRow> = {},
  ): MailFrame {
    const next: MailRow = { ...row, ...extra, state, reason };
    if (state === "held") {
      next.commands = commands(row.id);
    } else {
      delete next.commands;
    }
    this.rows.set(row.id, next);
    this.seq += 1;
    this.lastNonce = `nonce-${this.seq}`;
    const frame: MailFrame = {
      ...next,
      event: "mail",
      seq: this.seq,
      nonce: this.lastNonce,
      prev_state: prev,
      at: "2026-01-01T00:00:00.000Z",
    };
    this.log.push(frame);
    return frame;
  }

  accept(from: string, to: string, state: DeliveryState = "queued"): MailFrame {
    const msgSeq = this.nextMessage;
    this.nextMessage += 1;
    const id = `m${String(msgSeq).padStart(6, "0")}`;
    const row: MailRow = {
      msg_seq: msgSeq,
      id,
      from,
      to,
      conversation: "c",
      in_reply_to: null,
      hop: 0,
      act: "inform",
      task: null,
      exposure: null,
      state,
      turn_started: false,
      incarnation: null,
      attempts: 0,
      reason: null,
    };
    return this.emit(row, null, state, state === "held" ? "hop limit" : null);
  }

  /** A transition to `state`, or a metadata-only change when `state` is current. */
  move(id: string, state: DeliveryState, reason: string | null = null) {
    const row = this.row(id);
    return this.emit(row, row.state, state, reason, {
      attempts: row.attempts + (state === "offered" ? 1 : 0),
    });
  }

  ids(state?: DeliveryState): string[] {
    return [...this.rows.values()]
      .filter((row) => state === undefined || row.state === state)
      .map((row) => row.id);
  }

  head(): number {
    return this.seq;
  }

  anchor(): Anchor | null {
    return this.lastNonce === null
      ? null
      : { seq: this.seq, nonce: this.lastNonce };
  }

  private window(states: readonly DeliveryState[], bound: number) {
    const all = [...this.rows.values()]
      .filter((row) => states.includes(row.state))
      .sort((a, b) => b.msg_seq - a.msg_seq);
    return { rows: all.slice(0, bound), truncated: all.length > bound };
  }

  /** What `GET /mail?states=…&limit=1000` reads right now. */
  read(states: readonly DeliveryState[]): MailRow[] {
    return this.window(states, 1000).rows.map((row) => ({ ...row }));
  }

  edges(): EdgeCounts[] {
    const edges = new Map<string, EdgeCounts>();
    for (const row of this.rows.values()) {
      const key = `${row.from}\n${row.to}`;
      const edge = edges.get(key) ?? {
        from: row.from,
        to: row.to,
        queued: 0,
        offered: 0,
        consumed: 0,
        uncertain: 0,
        held: 0,
        dead: 0,
      };
      edge[row.state] += 1;
      edges.set(key, edge);
    }
    return [...edges.values()];
  }

  mail(): FullMail {
    const stuck = this.window(["held", "uncertain"], 1000);
    const moving = this.window(["queued", "offered"], 1000);
    return {
      anchor: this.anchor(),
      edges: this.edges(),
      edges_truncated: false,
      stuck: stuck.rows.map((row) => ({ ...row })),
      moving: moving.rows.map((row) => ({ ...row })),
      stuck_truncated: stuck.truncated,
      moving_truncated: moving.truncated,
    };
  }

  snapshot(): SnapshotFrame {
    return {
      event: "snapshot",
      mail_cursor: this.seq,
      agents: [],
      agents_truncated: false,
      approvals: [],
      approvals_truncated: false,
      mail: this.mail(),
    };
  }

  /** A valid resume, or a lag on a live connection: no mail state. */
  nullSnapshot(cursor: number): SnapshotFrame {
    return { ...this.snapshot(), mail_cursor: cursor, mail: null };
  }
}
