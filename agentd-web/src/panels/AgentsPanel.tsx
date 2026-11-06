import { useMemo, useState } from "react";
import { GlyphIcon } from "../graph/Glyph";
import type { Selection } from "../graph/Graph";
import { type GraphModel, HUMAN, PHASE_LABEL, phaseRank } from "../graph/model";
import styles from "./panels.module.css";

/**
 * Every agent as a plain list: the keyboard path through a large graph, and the
 * readable one in a narrow window.
 */
export function AgentsPanel({
  model,
  selection,
  onSelect,
}: {
  model: GraphModel;
  selection: Selection;
  onSelect(selection: Selection): void;
}) {
  const [filter, setFilter] = useState("");
  const rows = useMemo(() => {
    const needle = filter.trim().toLowerCase();
    return model.nodes
      .filter(
        (node) =>
          node.id !== HUMAN &&
          (needle === "" ||
            node.label.toLowerCase().includes(needle) ||
            node.kind.toLowerCase().includes(needle) ||
            PHASE_LABEL[node.phase].includes(needle)),
      )
      .sort(
        (a, b) =>
          phaseRank(a.phase) - phaseRank(b.phase) ||
          b.permissions + b.held - (a.permissions + a.held) ||
          a.label.localeCompare(b.label),
      );
  }, [model.nodes, filter]);
  return (
    <div data-testid="agents">
      <input
        className={styles.filter}
        type="search"
        placeholder="Filter by name, kind or phase"
        aria-label="Filter agents"
        value={filter}
        onChange={(event) => setFilter(event.target.value)}
      />
      {rows.length === 0 ? (
        <p className={styles.note}>No agents match.</p>
      ) : (
        <ul className={styles.agentList}>
          {rows.map((node) => {
            const selected =
              selection?.kind === "agent" && selection.id === node.id;
            return (
              <li key={node.id}>
                <button
                  type="button"
                  className={`${styles.agentRow} ${selected ? styles.agentRowSelected : ""}`}
                  aria-pressed={selected}
                  data-testid="agent-row"
                  onClick={() => onSelect({ kind: "agent", id: node.id })}
                >
                  <GlyphIcon phase={node.phase} />
                  <span className={styles.agentName}>{node.label}</span>
                  <span className={styles.agentPhase}>
                    {PHASE_LABEL[node.phase]}
                  </span>
                  <span className={styles.agentCounts}>
                    {node.permissions > 0 ? `${node.permissions} perm ` : ""}
                    {node.held > 0 ? `${node.held} held ` : ""}
                    {node.inbound > 0 ? `${node.inbound} queued` : ""}
                  </span>
                </button>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}
