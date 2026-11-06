import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
// The theme only: the rest of infinity-ui, including its Markdown renderer, is not
// a dependency of this app. It must be imported before the component styles.
import "../../infinity-ui/src/theme.css";
import { App, TokenScreen } from "./App";
import { FeedClient } from "./feed/client";
import { type TokenResult, forgetToken, takeToken } from "./feed/token";

function followColorScheme() {
  const query = window.matchMedia("(prefers-color-scheme: light)");
  const apply = () => {
    document.documentElement.dataset.theme = query.matches ? "light" : "dark";
  };
  apply();
  query.addEventListener("change", apply);
}

function readToken(): TokenResult {
  try {
    return takeToken({
      location: window.location,
      history: window.history,
      storage: window.sessionStorage,
    });
  } catch (error) {
    // Storage can be blocked. Without it the token has nowhere safe to stay.
    console.error("could not read the token", error);
    return { kind: "missing" };
  }
}

followColorScheme();
const root = createRoot(document.getElementById("root")!);
const token = readToken();
if (token.kind === "token") {
  const client = new FeedClient({
    token: token.token,
    host: window.location.host,
    createSocket: (url, protocols) => new WebSocket(url, protocols),
    fetch: (target, init) => window.fetch(target, init),
    now: () => Date.now(),
    random: () => Math.random(),
    setTimeout: (callback, ms) => window.setTimeout(callback, ms),
    clearTimeout: (handle) => window.clearTimeout(handle),
    log: (message, error) => console.warn(message, error),
    onTokenRejected: () => {
      try {
        forgetToken({ storage: window.sessionStorage });
      } catch (error) {
        console.error("could not forget the token", error);
      }
    },
  });
  root.render(
    <StrictMode>
      <App client={client} />
    </StrictMode>,
  );
} else {
  root.render(
    <TokenScreen
      reason={token.kind === "malformed" ? "rejected" : "missing"}
    />,
  );
}
