import {
  type PointerEvent as ReactPointerEvent,
  memo,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import styles from "./Graph.module.css";
import { Glyph, GlyphIcon, glyphClass } from "./Glyph";
import {
  type Point,
  StableLayout,
  type View,
  edgeGeometry,
  keepOrFit,
  zoomAround,
} from "./layout";
import {
  type EdgeStatus,
  type GraphEdge,
  type GraphModel,
  type GraphNode,
  PHASE_LABEL,
  edgesToDraw,
  isBroken,
} from "./model";

export type Selection =
  | { kind: "agent"; id: string }
  | { kind: "edge"; from: string; to: string }
  | null;

const EDGE_DRAW_LIMIT = 2000;
const ALL_LABELS_UP_TO = 40;
const BROKEN_TAGS_UP_TO = 24;
const LABEL_MAX_CHARS = 22;
const RECENT_MS = 2000;

const STATUS_CLASS: Record<EdgeStatus, string> = {
  stuck: styles.edgeStuck!,
  live: styles.edgeLive!,
  dead: styles.edgeDead!,
  calm: styles.edgeCalm!,
};

function nodeRadius(count: number): number {
  if (count <= ALL_LABELS_UP_TO) return 15;
  if (count <= 120) return 11;
  return 8;
}

function clip(text: string): string {
  const chars = Array.from(text);
  return chars.length > LABEL_MAX_CHARS
    ? `${chars.slice(0, LABEL_MAX_CHARS - 1).join("")}…`
    : text;
}

function edgeWidth(total: number): number {
  return 1 + Math.min(5, Math.log2(1 + total) * 0.7);
}

function edgeSummary(edge: GraphEdge, from: string, to: string): string {
  const { queued, offered, consumed, held, uncertain, dead } = edge.counts;
  return `${from} to ${to}: ${held} held, ${uncertain} uncertain, ${queued} queued, ${offered} offered, ${consumed} consumed, ${dead} dead`;
}

interface NodeViewProps {
  node: GraphNode;
  x: number;
  y: number;
  worldX: number;
  worldY: number;
  r: number;
  showLabel: boolean;
  /** Whether a broken node says its state in words. Its shape always does. */
  showTag: boolean;
  /** Shrinks the glyph in a crowded view, so the nodes stay apart. */
  scale: number;
  selected: boolean;
  dimmed: boolean;
  onSelect(id: string): void;
  onHover(id: string | null): void;
  focusable: boolean;
}

const NodeView = memo(function NodeView({
  node,
  x,
  y,
  worldX,
  worldY,
  r,
  showLabel,
  showTag,
  scale,
  selected,
  dimmed,
  onSelect,
  onHover,
  focusable,
}: NodeViewProps) {
  const broken = isBroken(node.phase);
  const description = `${node.label}, ${PHASE_LABEL[node.phase]}${
    node.permissions > 0 ? `, ${node.permissions} permissions pending` : ""
  }${node.held > 0 ? `, ${node.held} held` : ""}`;
  return (
    <g
      className={[
        styles.node,
        glyphClass(node.phase),
        selected ? styles.selected : "",
        dimmed ? styles.dimmed : "",
      ].join(" ")}
      transform={`translate(${x.toFixed(1)} ${y.toFixed(1)})`}
      data-testid="node"
      data-node-id={node.id}
      data-phase={node.phase}
      data-x={worldX.toFixed(3)}
      data-y={worldY.toFixed(3)}
      role="button"
      tabIndex={focusable ? 0 : -1}
      aria-label={description}
      aria-pressed={selected}
      onClick={(event) => {
        event.stopPropagation();
        onSelect(node.id);
      }}
      onKeyDown={(event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onSelect(node.id);
        }
      }}
      onPointerEnter={() => onHover(node.id)}
      onPointerLeave={() => onHover(null)}
    >
      <title>{description}</title>
      {selected ? <circle className={styles.ring} r={r * 1.9} /> : null}
      <g transform={scale === 1 ? undefined : `scale(${scale})`}>
        <Glyph phase={node.phase} r={r} />
        {node.permissions > 0 ? (
          <g
            className={styles.waiting}
            transform={`translate(${r * 1.05} ${-r * 1.05})`}
            data-testid="node-waiting"
          >
            <circle r={8} />
            <text dy="0.36em">?</text>
          </g>
        ) : null}
      </g>
      {showLabel || (broken && showTag) ? (
        <text className={styles.label} y={(r * 1.75 + 10) * scale}>
          {clip(node.label)}
        </text>
      ) : null}
      {broken && (showTag || showLabel) ? (
        <text className={styles.brokenTag} y={(r * 1.75 + 10) * scale + 13}>
          {node.phase === "failed" ? "FAILED" : "NEEDS RECOVERY"}
        </text>
      ) : null}
    </g>
  );
});

function Legend() {
  const [open, setOpen] = useState(false);
  return (
    <div className={styles.legend}>
      <button
        type="button"
        className={styles.legendToggle}
        aria-expanded={open}
        onClick={() => setOpen(!open)}
      >
        Legend
      </button>
      {open ? (
        <div className={styles.legendBody} data-testid="legend">
          {(
            [
              "failed",
              "needs-recovery",
              "crashed",
              "busy",
              "idle",
              "starting",
              "dormant",
              "stopped",
              "ended",
              "unknown",
              "human",
            ] as const
          ).map((phase) => (
            <div className={styles.legendRow} key={phase}>
              <GlyphIcon phase={phase} />
              <span>{PHASE_LABEL[phase]}</span>
            </div>
          ))}
          {(
            [
              ["stuck", "held or uncertain mail"],
              ["live", "queued or offered mail"],
              ["calm", "delivered mail"],
              ["dead", "dead mail only"],
            ] as const
          ).map(([status, text]) => (
            <div className={styles.legendRow} key={status}>
              <svg width={28} height={10} aria-hidden="true">
                <line
                  className={`${styles.edge} ${STATUS_CLASS[status]}`}
                  x1={1}
                  y1={5}
                  x2={27}
                  y2={5}
                  strokeWidth={2.5}
                />
              </svg>
              <span>{text}</span>
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}

export interface GraphProps {
  model: GraphModel;
  touched: Record<string, number>;
  selection: Selection;
  onSelect(selection: Selection): void;
  stale: boolean;
}

export function Graph({
  model,
  touched,
  selection,
  onSelect,
  stale,
}: GraphProps) {
  const container = useRef<HTMLDivElement>(null);
  const layout = useRef(new StableLayout());
  const [size, setSize] = useState({ width: 800, height: 600 });
  const [userView, setUserView] = useState<View | null>(null);
  const [layoutEpoch, setLayoutEpoch] = useState(0);
  const [fitEpoch, setFitEpoch] = useState(0);
  const [hovered, setHovered] = useState<string | null>(null);
  const [now, setNow] = useState(0);

  const { nodes, edges } = model;
  const nodeKey = useMemo(() => nodes.map((n) => n.id).join("\n"), [nodes]);

  // Runs only when the set of nodes changes. Traffic, phases and new edges
  // between known nodes never reach the layout.
  const points = useMemo(() => {
    // The edges that matter first, with no selection: where a node lands must not
    // depend on what the operator has selected.
    const links = edgesToDraw(edges, EDGE_DRAW_LIMIT, null).map((edge) => ({
      source: edge.from,
      target: edge.to,
    }));
    layout.current.update(
      nodes.map((n) => n.id),
      links,
    );
    const placed: Point[] = [];
    for (const node of nodes) {
      const point = layout.current.get(node.id);
      if (point) {
        placed.push(point);
      }
    }
    return placed;
  }, [nodeKey, layoutEpoch]);

  // `fitEpoch` asks for a fresh fit. New nodes otherwise leave the view alone
  // while it still shows everything, so an arrival does not shift the rest.
  const lastFit = useRef<{
    view: View;
    width: number;
    height: number;
    count: number;
    epoch: number;
  } | null>(null);
  const fit = useMemo(() => {
    const previous = lastFit.current;
    const next = keepOrFit(
      previous?.epoch === fitEpoch ? previous : null,
      points,
      size.width,
      size.height,
      72,
    );
    lastFit.current = {
      view: next,
      width: size.width,
      height: size.height,
      count: points.length,
      epoch: fitEpoch,
    };
    return next;
  }, [points, size, fitEpoch]);
  const view = userView ?? fit;
  const viewRef = useRef(view);
  viewRef.current = view;

  useLayoutEffect(() => {
    const element = container.current;
    if (!element) {
      return;
    }
    const measure = () => {
      const { width, height } = element.getBoundingClientRect();
      if (width > 0 && height > 0) {
        setSize({ width, height });
      }
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  // A wheel listener has to be registered by hand to be allowed to
  // `preventDefault`: React's are passive.
  useEffect(() => {
    const element = container.current;
    if (!element) {
      return;
    }
    const onWheel = (event: WheelEvent) => {
      event.preventDefault();
      const box = element.getBoundingClientRect();
      setUserView(
        zoomAround(
          viewRef.current,
          Math.exp(-event.deltaY * 0.0015),
          event.clientX - box.left,
          event.clientY - box.top,
        ),
      );
    };
    element.addEventListener("wheel", onWheel, { passive: false });
    return () => element.removeEventListener("wheel", onWheel);
  }, []);

  // Clears the highlight on edges that carried a frame in the last two seconds.
  const lastTouch = useMemo(
    () => Math.max(0, ...Object.values(touched).slice(-512)),
    [touched],
  );
  useEffect(() => {
    if (lastTouch === 0) {
      return;
    }
    setNow(Date.now());
    const timer = window.setTimeout(() => setNow(Date.now()), RECENT_MS + 50);
    return () => window.clearTimeout(timer);
  }, [lastTouch]);

  const drag = useRef<{
    id: number;
    x: number;
    y: number;
    start: View;
  } | null>(null);
  const onPointerDown = (event: ReactPointerEvent<SVGSVGElement>) => {
    if (event.button !== 0) {
      return;
    }
    drag.current = {
      id: event.pointerId,
      x: event.clientX,
      y: event.clientY,
      start: view,
    };
  };
  const onPointerMove = (event: ReactPointerEvent<SVGSVGElement>) => {
    const active = drag.current;
    if (!active || active.id !== event.pointerId) {
      return;
    }
    const dx = event.clientX - active.x;
    const dy = event.clientY - active.y;
    if (Math.hypot(dx, dy) < 4) {
      return;
    }
    event.currentTarget.setPointerCapture(event.pointerId);
    setUserView({
      k: active.start.k,
      tx: active.start.tx + dx,
      ty: active.start.ty + dy,
    });
  };
  const moved = useRef(false);
  const onPointerUp = (event: ReactPointerEvent<SVGSVGElement>) => {
    const active = drag.current;
    moved.current =
      active !== null &&
      Math.hypot(event.clientX - active.x, event.clientY - active.y) >= 4;
    drag.current = null;
  };

  const selectedNode = selection?.kind === "agent" ? selection.id : null;
  const selectedEdge =
    selection?.kind === "edge" ? `${selection.from}\n${selection.to}` : null;
  const focusIds = useMemo(() => {
    const focus = new Set<string>();
    const center = selectedNode ?? hovered;
    if (center !== null) {
      focus.add(center);
      for (const edge of edges) {
        if (edge.from === center) focus.add(edge.to);
        if (edge.to === center) focus.add(edge.from);
      }
    }
    if (selection?.kind === "edge") {
      focus.add(selection.from);
      focus.add(selection.to);
    }
    return focus;
  }, [edges, selectedNode, hovered, selection]);
  const focusing = selection !== null;

  const r = nodeRadius(nodes.length);
  const drawn = useMemo(
    () => edgesToDraw(edges, EDGE_DRAW_LIMIT, selectedNode),
    [edges, selectedNode],
  );
  const labels = new Map(nodes.map((n) => [n.id, n.label]));
  const showAllLabels = nodes.length <= ALL_LABELS_UP_TO || view.k >= 1;
  // In a crowded overview, words stay readable only while they are rare. The
  // shape, the header count and the panel still say a node is broken.
  const brokenCount = nodes.reduce(
    (n, node) => n + Number(isBroken(node.phase)),
    0,
  );
  const showBrokenTags = showAllLabels || brokenCount <= BROKEN_TAGS_UP_TO;
  const glyphScale = showAllLabels
    ? 1
    : Math.max(0.5, Math.min(1, view.k * 1.3));
  const screen = (point: Point): Point => ({
    x: point.x * view.k + view.tx,
    y: point.y * view.k + view.ty,
  });
  const canTabEdges = drawn.length <= 60;

  const edgeElements = drawn.map((edge) => {
    const a = layout.current.get(edge.from);
    const b = layout.current.get(edge.to);
    if (!a || !b) {
      return null;
    }
    const geometry = edgeGeometry(screen(a), screen(b), r, r);
    const from = labels.get(edge.from) ?? edge.from;
    const to = labels.get(edge.to) ?? edge.to;
    const summary = edgeSummary(edge, from, to);
    const selected = selectedEdge === edge.key;
    const related =
      selected ||
      (selectedNode !== null &&
        (edge.from === selectedNode || edge.to === selectedNode));
    const recent = now - (touched[edge.key] ?? -Infinity) < RECENT_MS;
    const { held, uncertain, queued, offered, consumed, dead } = edge.counts;
    return (
      <g
        key={edge.key}
        className={[
          styles.edgeGroup,
          selected ? styles.edgeSelected : "",
          focusing && !related ? styles.dimmed : "",
          recent ? styles.edgeRecent : "",
        ].join(" ")}
        data-testid="edge"
        data-from={edge.from}
        data-to={edge.to}
        data-status={edge.status}
        data-counts={`held=${held},uncertain=${uncertain},queued=${queued},offered=${offered},consumed=${consumed},dead=${dead}`}
        role="button"
        tabIndex={canTabEdges ? 0 : -1}
        aria-label={summary}
        aria-pressed={selected}
        onClick={(event) => {
          event.stopPropagation();
          onSelect({ kind: "edge", from: edge.from, to: edge.to });
        }}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            onSelect({ kind: "edge", from: edge.from, to: edge.to });
          }
        }}
      >
        <title>{summary}</title>
        <path
          className={`${styles.edge} ${STATUS_CLASS[edge.status]}`}
          d={geometry.d}
          strokeWidth={edgeWidth(edge.total)}
          markerEnd={`url(#arrow-${edge.status})`}
        />
        <path className={styles.edgeHit} d={geometry.d} />
        {edge.status === "stuck" ? (
          <g
            className={styles.stuckBadge}
            transform={`translate(${geometry.labelX.toFixed(1)} ${geometry.labelY.toFixed(1)})`}
          >
            <rect x={-20} y={-10} width={40} height={20} rx={4} />
            <text dy="0.36em">{`! ${compact(edge.stuck)}`}</text>
          </g>
        ) : edge.status === "live" && drawn.length <= 200 ? (
          <g
            className={styles.liveBadge}
            transform={`translate(${geometry.labelX.toFixed(1)} ${geometry.labelY.toFixed(1)})`}
          >
            <text dy="0.36em">{compact(edge.live)}</text>
          </g>
        ) : null}
      </g>
    );
  });

  return (
    <div
      className={`${styles.graph} ${stale ? styles.stale : ""}`}
      ref={container}
      data-testid="graph"
    >
      <svg
        className={styles.canvas}
        width={size.width}
        height={size.height}
        role="group"
        aria-label={`Agent graph: ${nodes.length} nodes, ${edges.length} edges`}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
        onPointerCancel={onPointerUp}
        onClick={() => {
          if (!moved.current) {
            onSelect(null);
          }
          moved.current = false;
        }}
      >
        <defs>
          {(["stuck", "live", "dead", "calm"] as const).map((status) => (
            <marker
              key={status}
              id={`arrow-${status}`}
              className={`${styles.arrow} ${STATUS_CLASS[status]}`}
              viewBox="0 0 10 10"
              refX="8"
              refY="5"
              markerWidth="9"
              markerHeight="9"
              markerUnits="userSpaceOnUse"
              orient="auto"
            >
              <path d="M 0 0 L 10 5 L 0 10 z" />
            </marker>
          ))}
        </defs>
        <g>{edgeElements}</g>
        <g>
          {nodes.map((node) => {
            const world = layout.current.get(node.id);
            if (!world) {
              return null;
            }
            const at = screen(world);
            return (
              <NodeView
                key={node.id}
                node={node}
                x={at.x}
                y={at.y}
                worldX={world.x}
                worldY={world.y}
                r={r}
                showLabel={showAllLabels || focusIds.has(node.id)}
                showTag={showBrokenTags}
                scale={focusIds.has(node.id) ? 1 : glyphScale}
                selected={selectedNode === node.id}
                dimmed={focusing && !focusIds.has(node.id)}
                onSelect={(id) => {
                  if (!moved.current) {
                    onSelect({ kind: "agent", id });
                  }
                  moved.current = false;
                }}
                onHover={setHovered}
                focusable={nodes.length <= 60}
              />
            );
          })}
        </g>
      </svg>
      <div className={styles.controls}>
        <button
          type="button"
          aria-label="Zoom in"
          onClick={() =>
            setUserView(zoomAround(view, 1.3, size.width / 2, size.height / 2))
          }
        >
          +
        </button>
        <button
          type="button"
          aria-label="Zoom out"
          onClick={() =>
            setUserView(
              zoomAround(view, 1 / 1.3, size.width / 2, size.height / 2),
            )
          }
        >
          −
        </button>
        <button
          type="button"
          onClick={() => {
            setUserView(null);
            setFitEpoch(fitEpoch + 1);
          }}
        >
          Fit
        </button>
        <button
          type="button"
          title="Lay the whole graph out again. Nodes will move."
          onClick={() => {
            layout.current.reset();
            setUserView(null);
            setFitEpoch(fitEpoch + 1);
            setLayoutEpoch(layoutEpoch + 1);
          }}
        >
          Re-layout
        </button>
      </div>
      <Legend />
      {edges.length > drawn.length ? (
        <div className={styles.edgeLimit} data-testid="edge-limit">
          Drawing {drawn.length.toLocaleString()} of{" "}
          {edges.length.toLocaleString()} edges: stuck and live first, then the
          busiest.
        </div>
      ) : null}
    </div>
  );
}

function compact(n: number): string {
  if (n < 1000) {
    return String(n);
  }
  // Never rounds up, so a badge cannot claim more than the count.
  const tenths = Math.floor(n / 100) / 10;
  return tenths < 100 ? `${tenths}k` : `${Math.floor(n / 1000)}k`;
}
