import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { type FeedClient, TokenRejected } from "../feed/client";
import { type ClientState, edgeKey } from "../feed/reducer";
import type { MailRow } from "../feed/types";
import { GlyphIcon } from "../graph/Glyph";
import type { Selection } from "../graph/Graph";
import {
  type GraphEdge,
  type GraphModel,
  HUMAN,
  PHASE_LABEL,
  displayName,
} from "../graph/model";
import { Command } from "./Command";
import styles from "./panels.module.css";

const EDGE_PAGE = 50;
/** A display-only view: it refreshes at most this often while traffic flows. */
const EDGE_REFRESH_MS = 2000;

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** `time`'s default encoding: `[year, ordinal, hour, minute, second, ns, ...]`. */
export function formatSentAt(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (Array.isArray(value) && value.length >= 5) {
    const [year, ordinal, hour, minute, second] = value as number[];
    const date = new Date(
      Date.UTC(year!, 0, ordinal!, hour!, minute!, second!),
    );
    if (!Number.isNaN(date.getTime())) {
      return date.toISOString().replace(".000Z", "Z");
    }
  }
  return "unknown";
}

function Counts({ edge }: { edge: GraphEdge }) {
  const { held, uncertain, queued, offered, consumed, dead } = edge.counts;
  return (
    <dl className={styles.counts} data-testid="edge-counts">
      {(
        [
          ["held", held],
          ["uncertain", uncertain],
          ["queued", queued],
          ["offered", offered],
          ["consumed", consumed],
          ["dead", dead],
        ] as const
      ).map(([name, n]) => (
        <div key={name}>
          <dt>{name}</dt>
          <dd>{n.toLocaleString("en-US")}</dd>
        </div>
      ))}
    </dl>
  );
}

function MessageRow({ row, client }: { row: MailRow; client: FeedClient }) {
  const [open, setOpen] = useState(false);
  const [body, setBody] = useState<
    | { kind: "loading" }
    | { kind: "error"; text: string }
    | { kind: "ready"; text: string; bytes: number; sentAt: string }
    | null
  >(null);
  const load = () => {
    const next = !open;
    setOpen(next);
    if (!next || body?.kind === "ready" || body?.kind === "loading") {
      return;
    }
    setBody({ kind: "loading" });
    client.message(row.id).then(
      (message) =>
        setBody({
          kind: "ready",
          text: message.body,
          bytes: message.bytes,
          sentAt: formatSentAt(message.sent_at),
        }),
      (error: unknown) => {
        if (!(error instanceof TokenRejected)) {
          console.error("loading a message body failed", error);
        }
        setBody({ kind: "error", text: errorText(error) });
      },
    );
  };
  return (
    <li className={styles.message} data-testid="edge-message" data-id={row.id}>
      <button
        type="button"
        className={styles.messageHead}
        aria-expanded={open}
        onClick={load}
      >
        <span
          className={`${styles.chip} ${styles[`state_${row.state}`] ?? ""}`}
        >
          {row.state}
        </span>
        <span className={styles.chip}>{row.act}</span>
        <span className={styles.messageMeta}>
          #{row.msg_seq} · hop {row.hop}
        </span>
      </button>
      {row.reason !== null ? (
        <p className={`${styles.untrusted} ${styles.reason}`}>{row.reason}</p>
      ) : null}
      {row.state === "held" ? (
        <>
          <Command
            label="Release"
            command={row.commands?.release ?? null}
            fallback="agentctl held"
          />
          <Command
            label="Drop"
            command={row.commands?.drop ?? null}
            fallback="agentctl held"
          />
        </>
      ) : null}
      {open && body?.kind === "loading" ? (
        <p className={styles.note}>Loading the body…</p>
      ) : null}
      {open && body?.kind === "error" ? (
        <p className={styles.errorNote}>Could not load the body: {body.text}</p>
      ) : null}
      {open && body?.kind === "ready" ? (
        <>
          <p className={styles.note}>
            Sent {body.sentAt} · {body.bytes.toLocaleString("en-US")} bytes
          </p>
          <pre
            className={`${styles.untrusted} ${styles.body}`}
            data-testid="message-body"
          >
            {body.text}
          </pre>
        </>
      ) : null}
    </li>
  );
}

function EdgeDetail({
  state,
  edge,
  from,
  to,
  client,
}: {
  state: ClientState;
  edge: GraphEdge | undefined;
  from: string;
  to: string;
  client: FeedClient;
}) {
  const [rows, setRows] = useState<MailRow[]>([]);
  const [next, setNext] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [failure, setFailure] = useState<string | null>(null);
  const [pagesLoaded, setPagesLoaded] = useState(1);
  const generation = useRef(0);

  // A short or empty page may still have a continuation, so a page is read until
  // it shows something or the listing ends. Never enters mail state.
  const read = useCallback(
    async (before: number | null, replace: boolean) => {
      const mine = ++generation.current;
      setLoading(true);
      setFailure(null);
      try {
        const found: MailRow[] = [];
        let cursor = before;
        for (;;) {
          const query = new URLSearchParams({
            from,
            to,
            limit: String(EDGE_PAGE),
          });
          if (cursor !== null) {
            query.set("before_msg_seq", String(cursor));
          }
          const page = await client.mailPage(query);
          if (generation.current !== mine) {
            return;
          }
          found.push(...page.rows);
          cursor = page.next_before_msg_seq;
          if (found.length > 0 || cursor === null) {
            break;
          }
        }
        setRows((current) => (replace ? found : [...current, ...found]));
        setNext(cursor);
        setLoading(false);
      } catch (error) {
        if (generation.current !== mine) {
          return;
        }
        if (!(error instanceof TokenRejected)) {
          console.error("listing an edge's messages failed", error);
        }
        setFailure(errorText(error));
        setLoading(false);
      }
    },
    [client, from, to],
  );

  useEffect(() => {
    setRows([]);
    setNext(null);
    setPagesLoaded(1);
    void read(null, true);
    return () => {
      generation.current += 1;
    };
  }, [read]);

  // While only the first page is showing, follow the traffic on this edge.
  const signature = edge
    ? `${edge.total}:${edge.stuck}:${edge.live}:${edge.counts.consumed}`
    : "";
  const firstSignature = useRef(signature);
  useEffect(() => {
    if (signature === firstSignature.current || pagesLoaded > 1) {
      return;
    }
    const timer = window.setTimeout(() => {
      firstSignature.current = signature;
      void read(null, true);
    }, EDGE_REFRESH_MS);
    return () => window.clearTimeout(timer);
  }, [signature, pagesLoaded, read]);

  return (
    <div data-testid="edge-detail">
      <h3 className={styles.detailTitle}>
        <span className={styles.untrusted}>{displayName(state, from)}</span>
        <span aria-hidden="true"> → </span>
        <span className={styles.untrusted}>{displayName(state, to)}</span>
      </h3>
      {edge ? <Counts edge={edge} /> : null}
      {failure !== null ? (
        <p className={styles.errorNote}>Could not list messages: {failure}</p>
      ) : null}
      {rows.length === 0 && !loading && failure === null ? (
        <p className={styles.note}>No messages on this edge.</p>
      ) : null}
      <ul className={styles.list}>
        {rows.map((row) => (
          <MessageRow key={row.id} row={row} client={client} />
        ))}
      </ul>
      {loading ? <p className={styles.note}>Loading…</p> : null}
      {next !== null && !loading ? (
        <button
          type="button"
          className={styles.more}
          onClick={() => {
            setPagesLoaded(pagesLoaded + 1);
            void read(next, false);
          }}
        >
          Load older messages
        </button>
      ) : null}
    </div>
  );
}

function AgentDetail({
  state,
  model,
  id,
  onSelect,
}: {
  state: ClientState;
  model: GraphModel;
  id: string;
  onSelect(selection: Selection): void;
}) {
  const node = model.nodes.find((candidate) => candidate.id === id);
  const queue = useMemo(
    () => state.moving.rows.filter((row) => row.to === id),
    [state.moving.rows, id],
  );
  const linked = useMemo(
    () => model.edges.filter((edge) => edge.from === id || edge.to === id),
    [model.edges, id],
  );
  const tail = useRef<HTMLPreElement>(null);
  const activity = state.activity[id] ?? "";
  useEffect(() => {
    const element = tail.current;
    if (element) {
      element.scrollTop = element.scrollHeight;
    }
  }, [activity]);
  if (!node) {
    return <p className={styles.note}>This agent is no longer listed.</p>;
  }
  const turn = state.turns[id];
  const denials = state.egress[id] ?? [];
  return (
    <div data-testid="agent-detail">
      <h3 className={styles.detailTitle}>
        <GlyphIcon phase={node.phase} />
        <span className={styles.untrusted}>{node.label}</span>
        <span className={styles.chip} data-testid="agent-phase">
          {PHASE_LABEL[node.phase]}
        </span>
      </h3>
      <p className={styles.note}>
        {node.kind ? `${node.kind} · ` : ""}
        {node.isolation ? `${node.isolation} · ` : ""}
        <code>{id}</code>
      </p>
      {node.error !== null ? (
        <p
          className={`${styles.untrusted} ${styles.errorNote}`}
          data-testid="detail-error"
        >
          {node.error}
        </p>
      ) : null}
      {turn ? <p className={styles.note}>Last turn: {turn}</p> : null}
      <h4 className={styles.detailHeading}>
        Queue{" "}
        <span className={styles.groupCount} data-testid="agent-queue-count">
          {node.inbound.toLocaleString("en-US")}
        </span>
      </h4>
      {queue.length === 0 ? (
        node.inbound > 0 ? (
          <p className={styles.note}>
            None of this agent's queue is among the newest 1,000 moving
            messages.
          </p>
        ) : (
          <p className={styles.note}>Empty.</p>
        )
      ) : (
        <ul className={styles.queue} data-testid="agent-queue">
          {queue.slice(0, 50).map((row) => (
            <li key={row.id}>
              <span
                className={`${styles.chip} ${styles[`state_${row.state}`] ?? ""}`}
              >
                {row.state}
              </span>{" "}
              <span className={styles.untrusted}>
                {displayName(state, row.from)}
              </span>{" "}
              <span className={styles.messageMeta}>
                {row.act} · #{row.msg_seq}
              </span>
            </li>
          ))}
          {node.inbound > queue.slice(0, 50).length ? (
            <li className={styles.note}>
              Showing {queue.slice(0, 50).length} of{" "}
              {node.inbound.toLocaleString("en-US")}.
            </li>
          ) : null}
        </ul>
      )}
      <h4 className={styles.detailHeading}>Activity</h4>
      {activity ? (
        <pre
          ref={tail}
          className={`${styles.untrusted} ${styles.body}`}
          data-testid="activity"
        >
          {activity}
        </pre>
      ) : (
        <p className={styles.note}>Nothing since this page opened.</p>
      )}
      {denials.length > 0 ? (
        <>
          <h4 className={styles.detailHeading}>Blocked network requests</h4>
          <ul className={styles.queue} data-testid="egress">
            {denials.map((denial, index) => (
              <li key={`${denial.at}-${index}`} className={styles.untrusted}>
                {denial.host}:{denial.port}
              </li>
            ))}
          </ul>
        </>
      ) : null}
      {linked.length > 0 ? (
        <>
          <h4 className={styles.detailHeading}>Connections</h4>
          <ul className={styles.queue}>
            {linked.slice(0, 100).map((edge) => (
              <li key={edgeKey(edge.from, edge.to)}>
                <button
                  type="button"
                  className={styles.agentLink}
                  onClick={() =>
                    onSelect({ kind: "edge", from: edge.from, to: edge.to })
                  }
                >
                  {edge.from === id
                    ? `→ ${displayName(state, edge.to)}`
                    : `← ${displayName(state, edge.from)}`}
                </button>{" "}
                <span className={styles.messageMeta}>
                  {edge.stuck > 0 ? `${edge.stuck} stuck · ` : ""}
                  {edge.total.toLocaleString("en-US")} total
                </span>
              </li>
            ))}
          </ul>
        </>
      ) : null}
    </div>
  );
}

export function DetailPanel({
  state,
  model,
  selection,
  client,
  onSelect,
}: {
  state: ClientState;
  model: GraphModel;
  selection: NonNullable<Selection>;
  client: FeedClient;
  onSelect(selection: Selection): void;
}) {
  const key =
    selection.kind === "agent"
      ? selection.id
      : edgeKey(selection.from, selection.to);
  // In a narrow window the graph and this panel are far apart on the page.
  const section = useRef<HTMLElement>(null);
  useEffect(() => {
    section.current?.scrollIntoView({ block: "nearest" });
  }, [key]);
  return (
    <section
      ref={section}
      className={styles.detail}
      aria-label="Selection"
      data-testid="detail"
    >
      <button
        type="button"
        className={styles.close}
        aria-label="Close selection"
        onClick={() => onSelect(null)}
      >
        ✕
      </button>
      {selection.kind === "agent" ? (
        selection.id === HUMAN ? (
          <div data-testid="human-detail">
            <h3 className={styles.detailTitle}>
              <GlyphIcon phase="human" />
              human
            </h3>
            <p className={styles.note}>
              You. Everything this page shows is read-only. Sending, approving
              and releasing stay on <code>agentctl</code>.
            </p>
          </div>
        ) : (
          <AgentDetail
            key={key}
            state={state}
            model={model}
            id={selection.id}
            onSelect={onSelect}
          />
        )
      ) : (
        <EdgeDetail
          key={key}
          state={state}
          edge={model.edges.find(
            (edge) => edge.from === selection.from && edge.to === selection.to,
          )}
          from={selection.from}
          to={selection.to}
          client={client}
        />
      )}
    </section>
  );
}
