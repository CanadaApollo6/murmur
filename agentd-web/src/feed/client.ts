// Drives one feed connection and the view-mode refetches from the reducer's
// plan. All decisions about state live in `reducer.ts`; this file owns sockets,
// timers and `fetch`, and its dependencies are injected so it can be tested with
// fakes.
import {
  type Action,
  type ClientState,
  type ListName,
  eventsTarget,
  initialState,
  listTarget,
  planFetches,
  reduce,
} from "./reducer";
import type { Frame, MailPage, MailRow, MessageBody } from "./types";
import { isMailRow, parseFrame } from "./wire";

const FLUSH_MS = 50;
const BACKOFF_MIN_MS = 500;
const BACKOFF_MAX_MS = 30_000;
const REQUEST_TIMEOUT_MS = 30_000;
/** Resyncs closer together than this back off, so a repeating fault cannot spin. */
const RESYNC_WINDOW_MS = 2 * BACKOFF_MAX_MS;
const SUBPROTOCOL = "murmur.events.v1";

export class TokenRejected extends Error {
  constructor() {
    super("the server rejected the token");
  }
}

interface SocketLike {
  onopen: ((event: Event) => void) | null;
  onmessage: ((event: MessageEvent) => void) | null;
  onclose: ((event: CloseEvent) => void) | null;
  onerror: ((event: Event) => void) | null;
  close(): void;
}

export interface ClientDeps {
  token: string;
  /** `location.host`, already validated by the server. */
  host: string;
  createSocket(url: string, protocols: string[]): SocketLike;
  fetch(target: string, init: RequestInit): Promise<Response>;
  now(): number;
  random(): number;
  setTimeout(callback: () => void, ms: number): number;
  clearTimeout(handle: number): void;
  log(message: string, error?: unknown): void;
  onTokenRejected(): void;
}

export interface Retry {
  attempt: number;
  /** The server answers HTTP but refused the feed, for example at its limit. */
  refused: boolean;
  /** What the feed sent that this page could not use, until a snapshot applies. */
  problem: string | null;
}

export class FeedClient {
  private state: ClientState = initialState();
  private retry: Retry = { attempt: 0, refused: false, problem: null };
  private resyncStreak = 0;
  private lastResyncAt: number | null = null;
  private readonly listeners = new Set<() => void>();
  private socket: SocketLike | null = null;
  private queue: Frame[] = [];
  private flushTimer: number | null = null;
  private retryTimer: number | null = null;
  private fetchTimer: number | null = null;
  private stopped = false;

  constructor(private readonly deps: ClientDeps) {}

  getState = (): ClientState => this.state;
  getRetry = (): Retry => this.retry;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  start() {
    this.stopped = false;
    this.connect();
  }

  stop() {
    this.stopped = true;
    this.clearTimers();
    const socket = this.socket;
    this.socket = null;
    socket?.close();
  }

  /** Skip the rest of the backoff. */
  retryNow() {
    if (
      this.stopped ||
      this.socket !== null ||
      this.state.link === "rejected"
    ) {
      return;
    }
    if (this.retryTimer !== null) {
      this.deps.clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    this.connect();
  }

  /** An authenticated read for display-only views. Never enters mail state. */
  async mailPage(query: URLSearchParams): Promise<MailPage> {
    return (await this.getJson(`/mail?${query.toString()}`)) as MailPage;
  }

  async message(id: string): Promise<MessageBody> {
    return (await this.getJson(
      `/messages/${encodeURIComponent(id)}`,
    )) as MessageBody;
  }

  private async getJson(target: string): Promise<unknown> {
    const response = await this.deps.fetch(target, {
      headers: { Authorization: `Bearer ${this.deps.token}` },
      // A request that never returns must not hold a list's one slot forever.
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
    if (response.status === 401) {
      this.tokenRejected();
      throw new TokenRejected();
    }
    if (!response.ok) {
      throw new Error(`${target}: HTTP ${response.status}`);
    }
    return await response.json();
  }

  private notify() {
    for (const listener of this.listeners) {
      try {
        listener();
      } catch (error) {
        this.deps.log("a feed listener failed", error);
      }
    }
  }

  private dispatch(action: Action) {
    try {
      this.state = reduce(this.state, action);
      this.pump();
    } catch (error) {
      this.resync("agentd sent data this page could not apply.", error);
      return;
    }
    this.notify();
  }

  /**
   * Drops the connection and the anchor, so the next connection gets a full
   * snapshot. A one-off fault costs one snapshot. A repeating one backs off and
   * stays on screen.
   */
  private resync(problem: string, error?: unknown) {
    this.deps.log(problem, error);
    const now = this.deps.now();
    this.resyncStreak =
      this.lastResyncAt !== null && now - this.lastResyncAt < RESYNC_WINDOW_MS
        ? this.resyncStreak + 1
        : 1;
    this.lastResyncAt = now;
    this.state = reduce(this.state, { type: "resync" });
    this.queue = [];
    if (this.flushTimer !== null) {
      this.deps.clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    this.retry = { attempt: this.resyncStreak - 1, refused: false, problem };
    const socket = this.socket;
    if (socket === null) {
      // `onclose` is already handling this connection, and retries when it is done.
      this.notify();
      return;
    }
    this.socket = null;
    socket.close();
    this.state = reduce(this.state, { type: "disconnected" });
    if (!this.stopped) {
      this.scheduleRetry();
    }
    this.notify();
  }

  private connect() {
    const url = `ws://${this.deps.host}${eventsTarget(this.state)}`;
    let socket: SocketLike;
    try {
      socket = this.deps.createSocket(url, [
        SUBPROTOCOL,
        `murmur.token.${this.deps.token}`,
      ]);
    } catch (error) {
      this.deps.log("could not create the feed socket", error);
      this.scheduleRetry();
      return;
    }
    this.socket = socket;
    let sawSnapshot = false;
    socket.onopen = () => {
      if (this.socket === socket) {
        this.dispatch({ type: "socket_open" });
      }
    };
    socket.onmessage = (event) => {
      if (this.socket !== socket) {
        return;
      }
      const parsed = parseFrame(event.data);
      if (parsed.kind === "unknown") {
        return;
      }
      if (parsed.kind === "invalid") {
        this.resync(`agentd sent ${parsed.reason}.`);
        return;
      }
      const { frame } = parsed;
      this.queue.push(frame);
      if (frame.event === "snapshot") {
        sawSnapshot = true;
        this.flush();
        if (this.socket === socket) {
          this.retry = { attempt: 0, refused: false, problem: null };
          this.notify();
        }
      } else if (this.flushTimer === null) {
        this.flushTimer = this.deps.setTimeout(() => {
          this.flushTimer = null;
          this.flush();
        }, FLUSH_MS);
      }
    };
    socket.onerror = () => {
      // Always followed by `onclose`, where it is handled.
    };
    socket.onclose = () => {
      if (this.socket !== socket) {
        return;
      }
      this.socket = null;
      // Everything received is applied first, so the anchor the next connection
      // echoes covers it.
      this.flush();
      this.dispatch({ type: "disconnected" });
      if (this.stopped) {
        return;
      }
      if (sawSnapshot) {
        this.scheduleRetry();
      } else {
        // A browser hides the status of a refused upgrade, so ask over HTTP.
        void this.diagnose();
      }
    };
  }

  private async diagnose() {
    let refused = false;
    try {
      const response = await this.deps.fetch("/mail?limit=1", {
        headers: { Authorization: `Bearer ${this.deps.token}` },
        // A probe that never returns must not stop the reconnection behind it.
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
      if (response.status === 401) {
        this.tokenRejected();
        return;
      }
      refused = response.ok;
    } catch (error) {
      this.deps.log("agentd is not reachable", error);
    }
    if (
      this.stopped ||
      this.socket !== null ||
      this.state.link === "rejected"
    ) {
      return;
    }
    this.scheduleRetry(refused);
  }

  private tokenRejected() {
    if (this.state.link === "rejected") {
      return;
    }
    this.clearTimers();
    this.queue = [];
    const socket = this.socket;
    this.socket = null;
    socket?.close();
    this.deps.onTokenRejected();
    this.dispatch({ type: "rejected" });
  }

  private scheduleRetry(refused = false) {
    if (this.retryTimer !== null) {
      return;
    }
    const attempt = this.retry.attempt + 1;
    this.retry = { attempt, refused, problem: this.retry.problem };
    const ceiling = Math.min(
      BACKOFF_MAX_MS,
      BACKOFF_MIN_MS * 2 ** (attempt - 1),
    );
    const delay = ceiling * (0.8 + 0.4 * this.deps.random());
    this.retryTimer = this.deps.setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, delay);
    this.notify();
  }

  private flush() {
    if (this.flushTimer !== null) {
      this.deps.clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    if (this.queue.length === 0) {
      return;
    }
    const frames = this.queue;
    this.queue = [];
    this.dispatch({ type: "frames", frames, now: this.deps.now() });
    if (this.state.resyncNeeded && this.socket !== null) {
      this.resync("This page missed part of agentd's mail feed.");
    }
  }

  /** Carries out the reducer's plan. Runs after every state change. */
  private pump() {
    if (this.fetchTimer !== null) {
      this.deps.clearTimeout(this.fetchTimer);
      this.fetchTimer = null;
    }
    const now = this.deps.now();
    const plan = planFetches(this.state, now);
    for (const { list, id } of plan.send) {
      this.state = reduce(this.state, { type: "fetch_sent", list, id, now });
      void this.refetch(list, id);
    }
    if (plan.wakeAt !== null) {
      this.fetchTimer = this.deps.setTimeout(
        () => {
          this.fetchTimer = null;
          this.pump();
          this.notify();
        },
        Math.max(0, plan.wakeAt - now),
      );
    }
  }

  private async refetch(list: ListName, id: number) {
    let rows: MailRow[];
    try {
      const page = (await this.getJson(listTarget(list))) as Partial<MailPage>;
      if (!Array.isArray(page.rows) || !page.rows.every(isMailRow)) {
        throw new Error(`${listTarget(list)}: a malformed page`);
      }
      rows = page.rows;
    } catch (error) {
      if (error instanceof TokenRejected) {
        return;
      }
      this.deps.log(`refetching ${list} failed`, error);
      this.dispatch({ type: "fetch_failed", list, id });
      return;
    }
    this.dispatch({ type: "fetch_ok", list, id, rows });
  }

  private clearTimers() {
    for (const timer of [this.flushTimer, this.retryTimer, this.fetchTimer]) {
      if (timer !== null) {
        this.deps.clearTimeout(timer);
      }
    }
    this.flushTimer = null;
    this.retryTimer = null;
    this.fetchTimer = null;
  }
}
