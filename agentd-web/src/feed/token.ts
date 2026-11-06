// The token path of section 5: the fragment never reaches a server or a
// `Referer`, so the page moves it to `sessionStorage` and strips it from the URL.

const STORAGE_KEY = "murmur.ui.token";
const TOKEN = /^[0-9a-fA-F]{64}$/;

export interface TokenEnvironment {
  location: { hash: string; pathname: string; search: string };
  history: { replaceState(data: unknown, unused: string, url: string): void };
  storage: {
    getItem(key: string): string | null;
    setItem(key: string, value: string): void;
    removeItem(key: string): void;
  };
}

export type TokenResult =
  | { kind: "token"; token: string }
  /** No token anywhere. */
  | { kind: "missing" }
  /** The URL carried something that cannot be a token. */
  | { kind: "malformed" };

export function takeToken(env: TokenEnvironment): TokenResult {
  const fragment = new URLSearchParams(env.location.hash.replace(/^#/, ""));
  const fromUrl = fragment.get("token");
  if (fromUrl !== null) {
    // Strip it whatever it holds: a token never stays in the address bar.
    env.history.replaceState(
      null,
      "",
      `${env.location.pathname}${env.location.search}`,
    );
    if (!TOKEN.test(fromUrl)) {
      env.storage.removeItem(STORAGE_KEY);
      return { kind: "malformed" };
    }
    env.storage.setItem(STORAGE_KEY, fromUrl);
    return { kind: "token", token: fromUrl };
  }
  const stored = env.storage.getItem(STORAGE_KEY);
  if (stored !== null && TOKEN.test(stored)) {
    return { kind: "token", token: stored };
  }
  return { kind: "missing" };
}

export function forgetToken(env: Pick<TokenEnvironment, "storage">) {
  env.storage.removeItem(STORAGE_KEY);
}
