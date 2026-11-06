import { describe, expect, it } from "vitest";
import { type TokenEnvironment, forgetToken, takeToken } from "./token";

const TOKEN = "ab".repeat(32);

function environment(hash: string, stored?: string) {
  const items = new Map<string, string>();
  if (stored !== undefined) {
    items.set("murmur.ui.token", stored);
  }
  const replaced: string[] = [];
  const env: TokenEnvironment = {
    location: { hash, pathname: "/", search: "" },
    history: {
      replaceState: (_data, _unused, url) => {
        replaced.push(url);
      },
    },
    storage: {
      getItem: (key) => items.get(key) ?? null,
      setItem: (key, value) => void items.set(key, value),
      removeItem: (key) => void items.delete(key),
    },
  };
  return { env, items, replaced };
}

describe("takeToken", () => {
  it("moves the fragment to sessionStorage and strips the URL", () => {
    const { env, items, replaced } = environment(`#token=${TOKEN}`);
    expect(takeToken(env)).toEqual({ kind: "token", token: TOKEN });
    expect(items.get("murmur.ui.token")).toBe(TOKEN);
    expect(replaced).toEqual(["/"]);
  });

  it("reads a stored token after a reload, and leaves the URL alone", () => {
    const { env, replaced } = environment("", TOKEN);
    expect(takeToken(env)).toEqual({ kind: "token", token: TOKEN });
    expect(replaced).toEqual([]);
  });

  it("prefers a new fragment over a stored token", () => {
    const newer = "cd".repeat(32);
    const { env, items } = environment(`#token=${newer}`, TOKEN);
    expect(takeToken(env)).toEqual({ kind: "token", token: newer });
    expect(items.get("murmur.ui.token")).toBe(newer);
  });

  it("strips and refuses a value that cannot be a token", () => {
    // A comma would otherwise throw inside `new WebSocket`.
    for (const bad of ["short", "x".repeat(64), `${TOKEN},evil`]) {
      const { env, items, replaced } = environment(
        `#token=${encodeURIComponent(bad)}`,
        TOKEN,
      );
      expect(takeToken(env)).toEqual({ kind: "malformed" });
      expect(items.has("murmur.ui.token")).toBe(false);
      expect(replaced).toEqual(["/"]);
    }
  });

  it("reports a missing token, and ignores a bad stored value", () => {
    expect(takeToken(environment("").env)).toEqual({ kind: "missing" });
    expect(takeToken(environment("", "junk").env)).toEqual({ kind: "missing" });
  });

  it("forgets", () => {
    const { env, items } = environment("", TOKEN);
    forgetToken(env);
    expect(items.size).toBe(0);
  });
});
