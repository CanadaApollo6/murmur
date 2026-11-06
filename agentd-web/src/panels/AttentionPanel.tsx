import { memo, useMemo, useState } from "react";
import { type ClientState, type MailList } from "../feed/reducer";
import type { MailRow, PermissionRequest } from "../feed/types";
import { GlyphIcon } from "../graph/Glyph";
import type { Selection } from "../graph/Graph";
import {
  type GraphModel,
  PHASE_LABEL,
  displayName,
  isBroken,
} from "../graph/model";
import { Command } from "./Command";
import styles from "./panels.module.css";

const PAGE = 100;
/** Enough to see a problem without pushing what waits on the operator off screen. */
const BROKEN_FIRST = 8;

const EFFECT_LABEL: Record<string, string> = {
  UserChoice: "choice",
  AllowOnce: "allow once",
  AllowAlways: "allow always",
  RejectOnce: "reject once",
  RejectAlways: "reject always",
};

function count(n: number): string {
  return n.toLocaleString("en-US");
}

const PermissionItem = memo(function PermissionItem({
  request,
  agentName,
  onSelect,
}: {
  request: PermissionRequest;
  agentName: string;
  onSelect(selection: Selection): void;
}) {
  return (
    <li className={styles.item} data-testid="permission">
      <div className={styles.itemHead}>
        <button
          type="button"
          className={styles.agentLink}
          onClick={() => onSelect({ kind: "agent", id: request.agent })}
        >
          {agentName}
        </button>
        <span className={styles.chip}>{request.tool}</span>
      </div>
      <p
        className={`${styles.untrusted} ${styles.title}`}
        data-testid="permission-title"
      >
        {request.title}
      </p>
      {request.detail ? (
        <details className={styles.details}>
          <summary>Details</summary>
          <pre className={styles.untrusted} data-testid="permission-detail">
            {request.detail}
          </pre>
        </details>
      ) : null}
      {request.options.map((option) => (
        <Command
          key={option.key}
          label={`${option.label} · ${EFFECT_LABEL[option.effect] ?? option.effect}`}
          command={option.command}
          fallback="agentctl approvals"
        />
      ))}
      {request.options_truncated ? (
        <p className={styles.note} data-testid="options-truncated">
          More options exist than are shown. Use <code>agentctl approvals</code>
          .
        </p>
      ) : null}
    </li>
  );
});

const StuckItem = memo(function StuckItem({
  row,
  fromName,
  toName,
  onSelect,
}: {
  row: MailRow;
  fromName: string;
  toName: string;
  onSelect(selection: Selection): void;
}) {
  return (
    <li
      className={styles.item}
      data-testid={`stuck-${row.state}`}
      data-id={row.id}
    >
      <div className={styles.itemHead}>
        <button
          type="button"
          className={styles.agentLink}
          onClick={() => onSelect({ kind: "edge", from: row.from, to: row.to })}
        >
          {fromName} → {toName}
        </button>
        <span className={styles.chip}>{row.act}</span>
        <span className={styles.chip}>hop {row.hop}</span>
      </div>
      {row.reason !== null ? (
        <p
          className={`${styles.untrusted} ${styles.reason}`}
          data-testid="stuck-reason"
        >
          {row.reason}
        </p>
      ) : null}
      {row.state === "held" ? (
        <>
          <Command
            label="Release"
            command={row.commands?.release ?? null}
            fallback="agentctl held"
            compact
          />
          <Command
            label="Drop"
            command={row.commands?.drop ?? null}
            fallback="agentctl held"
            compact
          />
        </>
      ) : null}
    </li>
  );
});

function Group({
  title,
  total,
  tone,
  testid,
  children,
}: {
  title: string;
  total: string;
  tone?: "danger" | "warn";
  testid: string;
  children: React.ReactNode;
}) {
  return (
    <section className={styles.group} data-testid={testid}>
      <h3
        className={`${styles.groupTitle} ${
          tone === "danger" ? styles.danger : tone === "warn" ? styles.warn : ""
        }`}
      >
        <span>{title}</span>
        <span className={styles.groupCount} data-testid={`${testid}-count`}>
          {total}
        </span>
      </h3>
      {children}
    </section>
  );
}

function ListFooter({
  list,
  shown,
  exact,
  edgesTruncated,
  noun,
}: {
  list: MailList;
  shown: number;
  exact: number;
  edgesTruncated: boolean;
  noun: string;
}) {
  if (list.mode === "feed") {
    return null;
  }
  return (
    <p className={styles.note} data-testid={`view-note-${noun}`}>
      This list is a window: {count(shown)} of{" "}
      {edgesTruncated ? "at least " : ""}
      {count(exact)} {noun}, newest first. The total comes from the edge counts.
      {list.dirty || list.outstanding !== null ? " Refreshing…" : ""}
    </p>
  );
}

function ShowMore({
  shown,
  total,
  onMore,
  step = PAGE,
}: {
  shown: number;
  total: number;
  onMore(): void;
  step?: number;
}) {
  if (shown >= total) {
    return null;
  }
  return (
    <button type="button" className={styles.more} onClick={onMore}>
      Show {count(Math.min(step, total - shown))} more of {count(total - shown)}
    </button>
  );
}

export function AttentionPanel({
  state,
  model,
  onSelect,
}: {
  state: ClientState;
  model: GraphModel;
  onSelect(selection: Selection): void;
}) {
  const { totals } = model;
  const [brokenShown, setBrokenShown] = useState(BROKEN_FIRST);
  const [heldShown, setHeldShown] = useState(PAGE);
  const [uncertainShown, setUncertainShown] = useState(PAGE);
  const broken = useMemo(
    () =>
      model.nodes
        .filter((node) => isBroken(node.phase) || node.phase === "crashed")
        .sort((a, b) => a.label.localeCompare(b.label)),
    [model.nodes],
  );
  const requests = Object.values(state.approvals);
  const held = useMemo(
    () => state.stuck.rows.filter((row) => row.state === "held"),
    [state.stuck.rows],
  );
  const uncertain = useMemo(
    () => state.stuck.rows.filter((row) => row.state === "uncertain"),
    [state.stuck.rows],
  );
  const plus = state.edgesTruncated ? "≥ " : "";
  const empty =
    broken.length === 0 &&
    requests.length === 0 &&
    totals.held === 0 &&
    totals.uncertain === 0 &&
    held.length === 0 &&
    uncertain.length === 0 &&
    !state.approvalsTruncated;
  if (empty) {
    return (
      <div className={styles.allClear} data-testid="attention-empty">
        <span className={styles.allClearMark} aria-hidden="true">
          ✓
        </span>
        Nothing is stuck and nothing is waiting on you.
      </div>
    );
  }
  return (
    <div data-testid="attention">
      {broken.length > 0 ? (
        <Group
          title="Agents in trouble"
          total={count(broken.length)}
          tone="danger"
          testid="broken"
        >
          <ul className={styles.list}>
            {broken.slice(0, brokenShown).map((node) => (
              <li
                className={styles.item}
                key={node.id}
                data-testid="broken-agent"
              >
                <div className={styles.itemHead}>
                  <GlyphIcon phase={node.phase} />
                  <button
                    type="button"
                    className={styles.agentLink}
                    onClick={() => onSelect({ kind: "agent", id: node.id })}
                  >
                    {node.label}
                  </button>
                  <span className={`${styles.chip} ${styles.chipDanger}`}>
                    {PHASE_LABEL[node.phase]}
                  </span>
                </div>
                {node.error !== null ? (
                  <p
                    className={`${styles.untrusted} ${styles.reason}`}
                    data-testid="agent-error"
                  >
                    {node.error}
                  </p>
                ) : null}
              </li>
            ))}
          </ul>
          <ShowMore
            shown={brokenShown}
            total={broken.length}
            onMore={() => setBrokenShown(brokenShown + PAGE)}
          />
        </Group>
      ) : null}
      {requests.length > 0 || state.approvalsTruncated ? (
        <Group
          title="Waiting on you: permissions"
          total={`${state.approvalsTruncated ? "≥ " : ""}${count(requests.length)}`}
          tone="warn"
          testid="permissions"
        >
          {state.approvalsTruncated ? (
            <p className={styles.note} data-testid="approvals-truncated">
              More requests exist than are shown. Use{" "}
              <code>agentctl approvals</code>.
            </p>
          ) : null}
          <ul className={styles.list}>
            {requests.map((request) => (
              <PermissionItem
                key={request.id}
                request={request}
                agentName={displayName(state, request.agent)}
                onSelect={onSelect}
              />
            ))}
          </ul>
        </Group>
      ) : null}
      {totals.held > 0 || held.length > 0 ? (
        <Group
          title="Waiting on you: held mail"
          total={`${plus}${count(totals.held)}`}
          tone="warn"
          testid="held"
        >
          <ListFooter
            list={state.stuck}
            shown={held.length}
            exact={totals.held}
            edgesTruncated={state.edgesTruncated}
            noun="held"
          />
          <ul className={styles.list}>
            {held.slice(0, heldShown).map((row) => (
              <StuckItem
                key={row.id}
                row={row}
                fromName={displayName(state, row.from)}
                toName={displayName(state, row.to)}
                onSelect={onSelect}
              />
            ))}
          </ul>
          <ShowMore
            shown={heldShown}
            total={held.length}
            onMore={() => setHeldShown(heldShown + PAGE)}
          />
        </Group>
      ) : null}
      {totals.uncertain > 0 || uncertain.length > 0 ? (
        <Group
          title="Uncertain delivery"
          total={`${plus}${count(totals.uncertain)}`}
          testid="uncertain"
        >
          <ListFooter
            list={state.stuck}
            shown={uncertain.length}
            exact={totals.uncertain}
            edgesTruncated={state.edgesTruncated}
            noun="uncertain"
          />
          <ul className={styles.list}>
            {uncertain.slice(0, uncertainShown).map((row) => (
              <StuckItem
                key={row.id}
                row={row}
                fromName={displayName(state, row.from)}
                toName={displayName(state, row.to)}
                onSelect={onSelect}
              />
            ))}
          </ul>
          <ShowMore
            shown={uncertainShown}
            total={uncertain.length}
            onMore={() => setUncertainShown(uncertainShown + PAGE)}
          />
        </Group>
      ) : null}
    </div>
  );
}
