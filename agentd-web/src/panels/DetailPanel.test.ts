import { describe, expect, it } from "vitest";
import { formatSentAt } from "./DetailPanel";

describe("formatSentAt", () => {
  it("reads the server's encoding of a time", () => {
    expect(formatSentAt([2026, 1, 12, 30, 15, 0, 0, 0, 0])).toBe(
      "2026-01-01T12:30:15Z",
    );
    expect(formatSentAt([2026, 32, 0, 0, 0, 0, 0, 0, 0])).toBe(
      "2026-02-01T00:00:00Z",
    );
  });

  it("passes a string through, and does not throw on anything else", () => {
    expect(formatSentAt("2026-01-01")).toBe("2026-01-01");
    expect(formatSentAt(null)).toBe("unknown");
    expect(formatSentAt([])).toBe("unknown");
  });
});
