import { describe, expect, it } from "vitest";
import {
  StableLayout,
  edgeGeometry,
  fitView,
  keepOrFit,
  zoomAround,
} from "./layout";

function ids(n: number): string[] {
  return Array.from({ length: n }, (_, i) => `agent:${i}`);
}

function positions(layout: StableLayout, list: string[]) {
  return new Map(list.map((id) => [id, { ...layout.get(id)! }]));
}

describe("StableLayout", () => {
  it("is deterministic for the same input", () => {
    const list = ids(12);
    const links = [{ source: list[0]!, target: list[1]! }];
    const a = new StableLayout();
    const b = new StableLayout();
    a.update(list, links);
    b.update(list, links);
    expect(positions(a, list)).toEqual(positions(b, list));
  });

  it("does not move anything when only edges change", () => {
    const list = ids(30);
    const layout = new StableLayout();
    layout.update(list, []);
    const before = positions(layout, list);
    layout.update(
      list,
      list.slice(1).map((id) => ({ source: list[0]!, target: id })),
    );
    expect(positions(layout, list)).toEqual(before);
  });

  it("does not move known nodes when a node arrives", () => {
    const list = ids(30);
    const layout = new StableLayout();
    layout.update(list, []);
    const before = positions(layout, list);
    const more = [...list, "agent:new"];
    layout.update(more, [{ source: "agent:new", target: list[0]! }]);
    expect(positions(layout, list)).toEqual(before);
    const placed = layout.get("agent:new")!;
    expect(Number.isFinite(placed.x) && Number.isFinite(placed.y)).toBe(true);
  });

  it("forgets removed nodes, and places them afresh if they return", () => {
    const list = ids(5);
    const layout = new StableLayout();
    layout.update(list, []);
    layout.update(list.slice(1), []);
    expect(layout.get(list[0]!)).toBeUndefined();
    layout.update(list, []);
    expect(layout.get(list[0]!)).toBeDefined();
  });

  it("keeps 200 nodes apart", () => {
    const list = ids(200);
    const layout = new StableLayout();
    layout.update(
      list,
      list.slice(1).map((id, i) => ({ source: list[i % 7]!, target: id })),
    );
    let closest = Infinity;
    for (let i = 0; i < list.length; i += 1) {
      for (let j = i + 1; j < list.length; j += 1) {
        const a = layout.get(list[i]!)!;
        const b = layout.get(list[j]!)!;
        closest = Math.min(closest, Math.hypot(a.x - b.x, a.y - b.y));
      }
    }
    expect(closest).toBeGreaterThan(40);
  });

  it("resets on request only", () => {
    const layout = new StableLayout();
    layout.update(["a"], []);
    layout.reset();
    expect(layout.get("a")).toBeUndefined();
  });
});

describe("view math", () => {
  it("fits every point inside the padding", () => {
    const points = [
      { x: -500, y: 40 },
      { x: 900, y: -300 },
      { x: 10, y: 700 },
    ];
    const view = fitView(points, 800, 600, 50);
    for (const { x, y } of points) {
      const sx = x * view.k + view.tx;
      const sy = y * view.k + view.ty;
      expect(sx).toBeGreaterThanOrEqual(49.9);
      expect(sx).toBeLessThanOrEqual(750.1);
      expect(sy).toBeGreaterThanOrEqual(49.9);
      expect(sy).toBeLessThanOrEqual(550.1);
    }
  });

  it("does not blow up a small graph", () => {
    expect(fitView([{ x: 0, y: 0 }], 800, 600, 50).k).toBeLessThanOrEqual(1.3);
    expect(fitView([], 800, 600, 50).k).toBe(1);
  });

  it("zooms around the pointer, within limits", () => {
    const view = { k: 1, tx: 10, ty: 20 };
    const zoomed = zoomAround(view, 2, 100, 100);
    const worldX = (100 - view.tx) / view.k;
    expect(worldX * zoomed.k + zoomed.tx).toBeCloseTo(100);
    expect(zoomAround(view, 1e9, 0, 0).k).toBe(4);
    expect(zoomAround(view, 1e-9, 0, 0).k).toBe(0.05);
  });
});

describe("keepOrFit", () => {
  const view = { k: 1, tx: 400, ty: 300 };
  const previous = { view, width: 800, height: 600, count: 3 };

  it("keeps a view that still shows every node", () => {
    expect(keepOrFit(previous, [{ x: 100, y: 100 }], 800, 600, 72)).toBe(view);
  });

  it("refits when a node would be out of sight", () => {
    const next = keepOrFit(previous, [{ x: 5000, y: 0 }], 800, 600, 72);
    expect(next).not.toBe(view);
    expect(5000 * next.k + next.tx).toBeLessThanOrEqual(800);
  });

  it("refits with no previous view, an empty one, or a new size", () => {
    const point = [{ x: 0, y: 0 }];
    expect(keepOrFit(null, point, 800, 600, 72)).not.toBe(view);
    expect(keepOrFit({ ...previous, count: 0 }, point, 800, 600, 72)).not.toBe(
      view,
    );
    expect(keepOrFit(previous, point, 801, 600, 72)).not.toBe(view);
  });
});

describe("edgeGeometry", () => {
  it("bows a pair of opposite edges to different sides", () => {
    const a = { x: 0, y: 0 };
    const b = { x: 200, y: 0 };
    const there = edgeGeometry(a, b, 10, 10);
    const back = edgeGeometry(b, a, 10, 10);
    expect(there.labelY).not.toBeCloseTo(back.labelY);
    expect(Math.sign(there.labelY)).toBe(-Math.sign(back.labelY));
  });

  it("draws a loop for mail an agent sent itself", () => {
    const loop = edgeGeometry({ x: 5, y: 5 }, { x: 5, y: 5 }, 10, 10);
    expect(loop.d.startsWith("M ")).toBe(true);
    expect(loop.d).not.toContain("NaN");
  });
});
