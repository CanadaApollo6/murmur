import { useEffect, useMemo, useState, useSyncExternalStore } from "react";
import styles from "./App.module.css";
import type { FeedClient } from "./feed/client";
import { Graph, type Selection } from "./graph/Graph";
import { buildGraph } from "./graph/model";
import { AgentsPanel } from "./panels/AgentsPanel";
import { AttentionPanel } from "./panels/AttentionPanel";
import { DetailPanel } from "./panels/DetailPanel";

function Stat({
  label,
  value,
  tone,
  testid,
}: {
  label: string;
  value: number;
  tone?: "danger" | "warn" | undefined;
  testid: string;
}) {
  return (
    <span
      className={`${styles.stat} ${
        value > 0 && tone === "danger"
          ? styles.statDanger
          : value > 0 && tone === "warn"
            ? styles.statWarn
            : ""
      }`}
      data-testid={testid}
    >
      <strong>{value.toLocaleString("en-US")}</strong> {label}
    </span>
  );
}

/** Shown instead of the console when no valid token is available. */
export function TokenScreen({ reason }: { reason: "missing" | "rejected" }) {
  return (
    <main
      className={styles.tokenScreen}
      data-testid="token-screen"
      data-reason={reason}
    >
      <div className={styles.tokenCard}>
        <h1>{reason === "rejected" ? "Token rejected" : "Token needed"}</h1>
        <p>
          {reason === "rejected"
            ? "agentd did not accept this page's token. It may have been rotated."
            : "This page needs a token to read from agentd."}
        </p>
        <p>
          Run <code>agentctl ui</code> and open the URL it prints.
        </p>
      </div>
    </main>
  );
}

export function App({ client }: { client: FeedClient }) {
  const state = useSyncExternalStore(client.subscribe, client.getState);
  const retry = useSyncExternalStore(client.subscribe, client.getRetry);
  const [selection, setSelection] = useState<Selection>(null);
  const [tab, setTab] = useState<"attention" | "agents">("attention");

  useEffect(() => {
    client.start();
    return () => client.stop();
  }, [client]);

  // Edges, agents and approvals are the only inputs, so an activity frame or a
  // refetch does not rebuild the graph.
  const model = useMemo(
    () => buildGraph(state),
    [state.agents, state.edges, state.approvals],
  );

  if (state.link === "rejected") {
    return <TokenScreen reason="rejected" />;
  }

  const { totals } = model;
  const firstLoad = !state.hasMail;
  const disconnected = state.link === "disconnected";
  const attentionCount =
    totals.failed +
    totals.needsRecovery +
    totals.crashed +
    totals.permissions +
    totals.held +
    totals.uncertain;
  return (
    <div
      className={styles.app}
      data-testid="app"
      data-link={state.link}
      data-cursor={state.cursor ?? ""}
      data-stuck-mode={state.stuck.mode}
      data-moving-mode={state.moving.mode}
    >
      <header className={styles.header}>
        <h1 className={styles.brand}>murmur</h1>
        <span
          className={`${styles.link} ${styles[`link_${state.link}`] ?? ""}`}
          data-testid="link"
          role="status"
        >
          <span className={styles.linkMark} aria-hidden="true">
            {state.link === "live"
              ? "●"
              : state.link === "connecting"
                ? "◌"
                : "✕"}
          </span>
          {state.link === "live"
            ? "Live"
            : state.link === "connecting"
              ? "Connecting"
              : "Disconnected"}
        </span>
        <div className={styles.stats}>
          <Stat
            label="failed"
            value={totals.failed}
            tone="danger"
            testid="stat-failed"
          />
          <Stat
            label={
              totals.needsRecovery === 1 ? "needs recovery" : "need recovery"
            }
            value={totals.needsRecovery}
            tone="danger"
            testid="stat-recovery"
          />
          <Stat
            label={totals.permissions === 1 ? "permission" : "permissions"}
            value={totals.permissions}
            tone="warn"
            testid="stat-permissions"
          />
          <Stat
            label="held"
            value={totals.held}
            tone="warn"
            testid="stat-held"
          />
          <Stat
            label="uncertain"
            value={totals.uncertain}
            tone="warn"
            testid="stat-uncertain"
          />
          <Stat
            label="queued"
            value={totals.queued + totals.offered}
            testid="stat-queued"
          />
          <Stat label="busy" value={totals.busy} testid="stat-busy" />
          <Stat
            label={totals.agents === 1 ? "agent" : "agents"}
            value={totals.agents}
            testid="stat-agents"
          />
        </div>
      </header>
      {disconnected ? (
        <div className={styles.banner} data-testid="disconnected" role="alert">
          <span>
            {retry.refused
              ? "agentd is reachable but refused the feed. It allows 8 feed subscribers."
              : "Disconnected from agentd."}{" "}
            {firstLoad
              ? "Nothing has loaded yet."
              : `Showing the last known state${
                  state.lastFrameAt === null
                    ? ""
                    : ` from ${new Date(state.lastFrameAt).toLocaleTimeString()}`
                }. It resumes without loss when the connection returns.`}{" "}
            Reconnect attempt {retry.attempt}.
          </span>
          <button type="button" onClick={() => client.retryNow()}>
            Retry now
          </button>
        </div>
      ) : null}
      {retry.problem !== null ? (
        <div className={styles.banner} data-testid="feed-problem" role="alert">
          <span>
            {retry.problem} Reloading everything from agentd. What is on screen
            may be out of date until then.
          </span>
        </div>
      ) : null}
      {state.agentsTruncated || state.edgesTruncated ? (
        <div className={styles.notice} data-testid="graph-truncated">
          {state.agentsTruncated
            ? "agentd has more agents than a snapshot lists (1,024). Use agentctl ls for all of them. "
            : ""}
          {state.edgesTruncated
            ? "agentd has more edges than a snapshot lists (65,536), so edge counts here are not exact."
            : ""}
        </div>
      ) : null}
      <div className={styles.main}>
        <section className={styles.graphArea} aria-label="Agent graph">
          {!firstLoad && model.nodes.length === 0 ? (
            <div className={styles.empty} data-testid="empty">
              <h2>No agents yet</h2>
              <p>
                They appear here as soon as they exist. Start one with{" "}
                <code>agentctl spawn</code>.
              </p>
            </div>
          ) : firstLoad ? (
            <div className={styles.empty} data-testid="loading">
              <p>{disconnected ? "Waiting for agentd…" : "Loading…"}</p>
            </div>
          ) : (
            <Graph
              model={model}
              touched={state.edgeTouched}
              selection={selection}
              onSelect={setSelection}
              stale={disconnected}
            />
          )}
        </section>
        <aside className={styles.rail}>
          <div className={styles.tabs} role="tablist">
            <button
              type="button"
              role="tab"
              aria-selected={tab === "attention"}
              className={tab === "attention" ? styles.tabActive : styles.tab}
              onClick={() => setTab("attention")}
            >
              Attention
              <span className={styles.tabCount}>
                {attentionCount.toLocaleString("en-US")}
              </span>
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={tab === "agents"}
              className={tab === "agents" ? styles.tabActive : styles.tab}
              onClick={() => setTab("agents")}
              data-testid="tab-agents"
            >
              Agents
              <span className={styles.tabCount}>
                {totals.agents.toLocaleString("en-US")}
              </span>
            </button>
          </div>
          <div className={styles.railBody}>
            {tab === "attention" ? (
              <AttentionPanel
                state={state}
                model={model}
                onSelect={setSelection}
              />
            ) : (
              <AgentsPanel
                model={model}
                selection={selection}
                onSelect={setSelection}
              />
            )}
          </div>
          {selection !== null ? (
            <div className={styles.railDetail}>
              <DetailPanel
                state={state}
                model={model}
                selection={selection}
                client={client}
                onSelect={setSelection}
              />
            </div>
          ) : null}
        </aside>
      </div>
    </div>
  );
}
