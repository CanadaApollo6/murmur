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

const FLUSH_MS = 50;
const BACKOFF_MIN_MS = 500;
const BACKOFF_MAX_MS = 30_000;
const REQUEST_TIMEOUT_MS = 30_000;
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
}

export class FeedClient {
  private state: ClientState = initialState();
  private retry: Retry = { attempt: 0, refused: false };
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

  private dispatch(action: Action) {
    this.state = reduce(this.state, action);
    this.pump();
    for (const listener of this.listeners) {
      listener();
    }
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
      const frame = this.parse(event.data);
      if (frame === null) {
        return;
      }
      this.queue.push(frame);
      if (frame.event === "snapshot") {
        sawSnapshot = true;
        this.retry = { attempt: 0, refused: false };
        this.flush();
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

  private parse(data: unknown): Frame | null {
    if (typeof data !== "string") {
      this.deps.log("ignoring a binary feed message");
      return null;
    }
    try {
      return JSON.parse(data) as Frame;
    } catch (error) {
      this.deps.log("ignoring a malformed feed message", error);
      return null;
    }
  }

  private async diagnose() {
    let refused = false;
    try {
      const response = await this.deps.fetch("/mail?limit=1", {
        headers: { Authorization: `Bearer ${this.deps.token}` },
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
    this.retry = { attempt, refused };
    for (const listener of this.listeners) {
      listener();
    }
    const ceiling = Math.min(
      BACKOFF_MAX_MS,
      BACKOFF_MIN_MS * 2 ** (attempt - 1),
    );
    const delay = ceiling * (0.8 + 0.4 * this.deps.random());
    this.retryTimer = this.deps.setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, delay);
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
          for (const listener of this.listeners) {
            listener();
          }
        },
        Math.max(0, plan.wakeAt - now),
      );
    }
  }

  private async refetch(list: ListName, id: number) {
    let rows: MailRow[];
    try {
      rows = ((await this.getJson(listTarget(list))) as MailPage).rows;
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
