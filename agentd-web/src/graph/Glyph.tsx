// One shape per state, so that state never relies on color alone. Failed and
// needs-recovery get a heavier silhouette and a heavy halo as well.
import styles from "./Graph.module.css";
import type { NodeKind } from "./model";

function polygon(sides: number, radius: number, rotate: number): string {
  const points: string[] = [];
  for (let i = 0; i < sides; i += 1) {
    const angle = rotate + (i * 2 * Math.PI) / sides;
    points.push(
      `${(radius * Math.cos(angle)).toFixed(2)},${(radius * Math.sin(angle)).toFixed(2)}`,
    );
  }
  return points.join(" ");
}

const PHASE_CLASS: Record<NodeKind, string> = {
  failed: styles.failed!,
  "needs-recovery": styles.recovery!,
  crashed: styles.crashed!,
  busy: styles.busy!,
  starting: styles.transition!,
  suspending: styles.transition!,
  idle: styles.idle!,
  dormant: styles.dormant!,
  ended: styles.ended!,
  stopped: styles.stopped!,
  unknown: styles.unknown!,
  human: styles.human!,
};

export function glyphClass(phase: NodeKind): string {
  return PHASE_CLASS[phase];
}

function Mark({ r, text }: { r: number; text: string }) {
  return (
    <text className={styles.glyphMark} fontSize={r * 1.05} dy="0.36em">
      {text}
    </text>
  );
}

/** Draws around (0, 0). The caller positions it. */
export function Glyph({ phase, r }: { phase: NodeKind; r: number }) {
  switch (phase) {
    case "failed":
      return (
        <>
          <circle className={styles.halo} r={r * 1.55} />
          <polygon
            className={styles.shape}
            points={polygon(8, r * 1.18, Math.PI / 8)}
          />
          <Mark r={r} text="✕" />
        </>
      );
    case "needs-recovery":
      return (
        <>
          <circle className={styles.halo} r={r * 1.55} />
          <polygon className={styles.shape} points={polygon(4, r * 1.4, 0)} />
          <Mark r={r} text="!" />
        </>
      );
    case "crashed":
      return (
        <>
          <polygon
            className={styles.shape}
            points={polygon(3, r * 1.3, -Math.PI / 2)}
          />
          <Mark r={r * 0.8} text="!" />
        </>
      );
    case "busy":
      return (
        <>
          <circle className={styles.spinner} r={r * 1.32} />
          <circle className={styles.shape} r={r} />
        </>
      );
    case "starting":
    case "suspending":
      return <circle className={styles.shape} r={r} />;
    case "idle":
      return <circle className={styles.shape} r={r} />;
    case "dormant":
      return (
        <>
          <circle className={styles.shape} r={r * 0.82} />
          <Mark r={r * 0.75} text="z" />
        </>
      );
    case "ended":
      return (
        <>
          <circle className={styles.shape} r={r * 0.82} />
          <line
            className={styles.slash}
            x1={-r * 0.58}
            y1={r * 0.58}
            x2={r * 0.58}
            y2={-r * 0.58}
          />
        </>
      );
    case "stopped":
      return (
        <polygon
          className={styles.shape}
          points={polygon(4, r * 1.05, Math.PI / 4)}
        />
      );
    case "unknown":
      return (
        <>
          <circle className={styles.shape} r={r * 0.82} />
          <Mark r={r * 0.75} text="?" />
        </>
      );
    case "human":
      return (
        <>
          <rect
            className={styles.shape}
            x={-r * 1.1}
            y={-r * 1.1}
            width={r * 2.2}
            height={r * 2.2}
            rx={r * 0.45}
          />
          <Mark r={r} text="H" />
        </>
      );
  }
}

/** A small standalone icon, for lists and the legend. */
export function GlyphIcon({
  phase,
  size = 18,
}: {
  phase: NodeKind;
  size?: number;
}) {
  return (
    <svg
      className={`${styles.icon} ${glyphClass(phase)}`}
      width={size}
      height={size}
      viewBox="-16 -16 32 32"
      aria-hidden="true"
    >
      <Glyph phase={phase} r={9} />
    </svg>
  );
}
