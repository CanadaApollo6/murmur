// A guard on the rule that matters most for a hostile feed: nothing in this app may
// turn agent-authored text into markup.
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

function sources(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) {
      return sources(path);
    }
    return /\.tsx?$/.test(name) && !name.endsWith(".test.ts") ? [path] : [];
  });
}

describe("sources", () => {
  const files = sources(new URL(".", import.meta.url).pathname);

  it("finds the app", () => {
    expect(files.length).toBeGreaterThan(5);
  });

  it("never builds markup or code from strings", () => {
    const banned =
      /dangerouslySetInnerHTML|\.innerHTML|\.outerHTML|insertAdjacentHTML|document\.write|\beval\(|new Function\(|createContextualFragment/;
    for (const file of files) {
      expect(banned.test(readFileSync(file, "utf8")), file).toBe(false);
    }
  });

  it("never loads anything from another origin", () => {
    const external = /https?:\/\/(?!www\.w3\.org)/;
    for (const file of files) {
      expect(external.test(readFileSync(file, "utf8")), file).toBe(false);
    }
  });

  it("never renders Markdown", () => {
    for (const file of files) {
      expect(
        /streamdown|marked|markdown-it|react-markdown/.test(
          readFileSync(file, "utf8"),
        ),
        file,
      ).toBe(false);
    }
  });
});
