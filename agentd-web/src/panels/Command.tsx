// Commands are built by the server and only ever shown or copied here. The
// browser never assembles one.
import { useEffect, useRef, useState } from "react";
import styles from "./panels.module.css";

type CopyState = "idle" | "copied" | "failed";

export function Command({
  label,
  command,
  fallback,
  compact = false,
}: {
  label: string;
  command: string | null;
  /** The `agentctl` subcommand to use when the server sent no command. */
  fallback: string;
  /** One scrolling line instead of wrapped text. */
  compact?: boolean;
}) {
  const [copy, setCopy] = useState<CopyState>("idle");
  const timer = useRef<number | null>(null);
  useEffect(
    () => () => {
      if (timer.current !== null) {
        window.clearTimeout(timer.current);
      }
    },
    [],
  );
  if (command === null) {
    return (
      <div className={styles.command} data-testid="no-command">
        <span className={styles.commandLabel}>{label}</span>
        <p className={styles.noCommand}>
          No copyable command for this value. Use <code>{fallback}</code>.
        </p>
      </div>
    );
  }
  const onCopy = async () => {
    try {
      await navigator.clipboard.writeText(command);
      setCopy("copied");
    } catch (error) {
      console.error("copying a command failed", error);
      setCopy("failed");
    }
    if (timer.current !== null) {
      window.clearTimeout(timer.current);
    }
    timer.current = window.setTimeout(() => setCopy("idle"), 2000);
  };
  return (
    <div
      className={compact ? styles.commandCompact : styles.command}
      data-testid="command"
    >
      <span className={styles.commandLabel}>{label}</span>
      <div className={styles.commandLine}>
        <code
          className={styles.commandText}
          data-testid="command-text"
          title={compact ? command : undefined}
        >
          {command}
        </code>
        <button
          type="button"
          className={styles.copy}
          data-testid="copy"
          onClick={() => void onCopy()}
        >
          {copy === "idle"
            ? "Copy"
            : copy === "copied"
              ? "Copied ✓"
              : "Select text"}
        </button>
      </div>
    </div>
  );
}
