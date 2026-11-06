// Node positions that do not jump. A force layout that reheats on every update
// reshuffles the graph under the operator's eyes, so the simulation here never
// runs freely after the first layout: it is run to completion synchronously,
// only when the set of nodes changes, with every node that already has a
// position pinned. Traffic, phases and new edges between known nodes do not touch
// it at all.
import {
  type SimulationLinkDatum,
  type SimulationNodeDatum,
  forceCollide,
  forceLink,
  forceManyBody,
  forceSimulation,
  forceX,
  forceY,
} from "d3-force";

export interface Point {
  x: number;
  y: number;
}

export interface LayoutLink {
  source: string;
  target: string;
}

interface SimNode extends SimulationNodeDatum {
  id: string;
}

function hash(text: string): number {
  let h = 2166136261;
  for (let i = 0; i < text.length; i += 1) {
    h = Math.imul(h ^ text.charCodeAt(i), 16777619);
  }
  return h >>> 0;
}

function seededRandom(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state / 4294967296;
  };
}

export class StableLayout {
  private positions = new Map<string, Point>();

  /** Forget every position. Only an explicit operator action does this. */
  reset() {
    this.positions = new Map();
  }

  get(id: string): Point | undefined {
    return this.positions.get(id);
  }

  /** Bring the layout up to date with `ids`. Known nodes keep their position. */
  update(ids: readonly string[], links: readonly LayoutLink[]): void {
    const wanted = new Set(ids);
    for (const id of this.positions.keys()) {
      if (!wanted.has(id)) {
        this.positions.delete(id);
      }
    }
    const fresh = ids.filter((id) => !this.positions.has(id));
    if (fresh.length === 0) {
      return;
    }
    const known = new Map(this.positions);
    const neighbours = new Map<string, string[]>();
    for (const { source, target } of links) {
      if (wanted.has(source) && wanted.has(target) && source !== target) {
        neighbours.set(source, [...(neighbours.get(source) ?? []), target]);
        neighbours.set(target, [...(neighbours.get(target) ?? []), source]);
      }
    }
    const spread = 40 * Math.sqrt(ids.length) + 60;
    const nodes: SimNode[] = ids.map((id) => {
      const position = known.get(id);
      if (position) {
        return {
          id,
          x: position.x,
          y: position.y,
          fx: position.x,
          fy: position.y,
        };
      }
      const anchor = (neighbours.get(id) ?? [])
        .map((other) => known.get(other))
        .find((p) => p !== undefined);
      const random = seededRandom(hash(id));
      const angle = random() * 2 * Math.PI;
      const distance = anchor ? 90 + random() * 40 : spread * (0.3 + random());
      return {
        id,
        x: (anchor?.x ?? 0) + Math.cos(angle) * distance,
        y: (anchor?.y ?? 0) + Math.sin(angle) * distance,
      };
    });
    const simLinks: SimulationLinkDatum<SimNode>[] = links
      .filter(
        ({ source, target }) =>
          source !== target && wanted.has(source) && wanted.has(target),
      )
      .map(({ source, target }) => ({ source, target }));
    const simulation = forceSimulation<SimNode>(nodes)
      .randomSource(seededRandom(ids.length))
      .force("charge", forceManyBody().strength(-340).distanceMax(900))
      .force(
        "link",
        forceLink<SimNode, SimulationLinkDatum<SimNode>>(simLinks)
          .id((node) => node.id)
          .distance(130)
          .strength(0.25),
      )
      .force("collide", forceCollide(46))
      .force("x", forceX(0).strength(0.05))
      .force("y", forceY(0).strength(0.05))
      .stop();
    const ticks = ids.length > 400 ? 120 : 300;
    for (let i = 0; i < ticks; i += 1) {
      simulation.tick();
    }
    for (const node of nodes) {
      if (!known.has(node.id)) {
        this.positions.set(node.id, { x: node.x ?? 0, y: node.y ?? 0 });
      }
    }
  }
}

export interface View {
  k: number;
  tx: number;
  ty: number;
}

/** The transform that fits `points` in a `width` by `height` area. */
/**
 * The fit to use after the node set changes. A view that still shows every node
 * stays as it is, because refitting moves every node on screen. It refits only
 * when a node would be out of sight, or when `previous` is null.
 */
export function keepOrFit(
  previous: { view: View; width: number; height: number; count: number } | null,
  points: readonly Point[],
  width: number,
  height: number,
  padding: number,
): View {
  if (
    previous !== null &&
    previous.count > 0 &&
    previous.width === width &&
    previous.height === height
  ) {
    const { view } = previous;
    const visible = points.every(({ x, y }) => {
      const sx = x * view.k + view.tx;
      const sy = y * view.k + view.ty;
      return (
        sx >= padding / 2 &&
        sx <= width - padding / 2 &&
        sy >= padding / 2 &&
        sy <= height - padding / 2
      );
    });
    if (visible) {
      return view;
    }
  }
  return fitView(points, width, height, padding);
}

export function fitView(
  points: readonly Point[],
  width: number,
  height: number,
  padding: number,
): View {
  if (points.length === 0) {
    return { k: 1, tx: width / 2, ty: height / 2 };
  }
  let minX = Infinity;
  let maxX = -Infinity;
  let minY = Infinity;
  let maxY = -Infinity;
  for (const { x, y } of points) {
    minX = Math.min(minX, x);
    maxX = Math.max(maxX, x);
    minY = Math.min(minY, y);
    maxY = Math.max(maxY, y);
  }
  const usableWidth = Math.max(1, width - 2 * padding);
  const usableHeight = Math.max(1, height - 2 * padding);
  const k = Math.min(
    1.3,
    usableWidth / Math.max(1, maxX - minX),
    usableHeight / Math.max(1, maxY - minY),
  );
  return {
    k,
    tx: width / 2 - (k * (minX + maxX)) / 2,
    ty: height / 2 - (k * (minY + maxY)) / 2,
  };
}

/** Zoom by `factor`, keeping the screen point (`cx`, `cy`) where it is. */
export function zoomAround(
  view: View,
  factor: number,
  cx: number,
  cy: number,
): View {
  const k = Math.min(4, Math.max(0.05, view.k * factor));
  const applied = k / view.k;
  return {
    k,
    tx: cx - (cx - view.tx) * applied,
    ty: cy - (cy - view.ty) * applied,
  };
}

export interface EdgeGeometry {
  d: string;
  labelX: number;
  labelY: number;
}

/**
 * A quadratic arc from `a` to `b`, trimmed to the node radii. The bow is always to
 * the same side of the direction of travel, so `a → b` and `b → a` do not overlap.
 */
export function edgeGeometry(
  a: Point,
  b: Point,
  radiusA: number,
  radiusB: number,
): EdgeGeometry {
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const length = Math.hypot(dx, dy);
  if (length < 1) {
    // A message an agent sent to itself.
    const r = radiusA + 10;
    return {
      d: `M ${a.x - radiusA * 0.6} ${a.y - radiusA * 0.8} C ${a.x - r * 2} ${a.y - r * 2.4}, ${a.x + r * 2} ${a.y - r * 2.4}, ${a.x + radiusA * 0.6} ${a.y - radiusA * 0.8 - 2}`,
      labelX: a.x,
      labelY: a.y - r * 1.9,
    };
  }
  const bow = Math.min(46, length * 0.14);
  const cx = (a.x + b.x) / 2 + (dy / length) * bow;
  const cy = (a.y + b.y) / 2 - (dx / length) * bow;
  const trim = (from: Point, radius: number): Point => {
    const ux = cx - from.x;
    const uy = cy - from.y;
    const ul = Math.max(1, Math.hypot(ux, uy));
    return { x: from.x + (ux / ul) * radius, y: from.y + (uy / ul) * radius };
  };
  const start = trim(a, radiusA + 1);
  const end = trim(b, radiusB + 5);
  return {
    d: `M ${start.x.toFixed(1)} ${start.y.toFixed(1)} Q ${cx.toFixed(1)} ${cy.toFixed(1)} ${end.x.toFixed(1)} ${end.y.toFixed(1)}`,
    labelX: 0.25 * start.x + 0.5 * cx + 0.25 * end.x,
    labelY: 0.25 * start.y + 0.5 * cy + 0.25 * end.y,
  };
}
