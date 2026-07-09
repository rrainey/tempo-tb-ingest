/**
 * Snapshot-first WebSocket client (design §3.7): the server's first frame is
 * a snapshot, then events. On drop or stream.gap: reconnect with backoff and
 * apply the fresh snapshot. The consumer is notified on every change and owns
 * rendering cadence.
 */

import type { Snapshot, WsFrame } from "./contract";
import { applyEvent, applySnapshot, initialViewModel, type ViewModel } from "./reducer";

export interface ClientOptions {
  url?: string; // default: ws(s)://<host>/events
  backoffInitialMs?: number;
  backoffMaxMs?: number;
}

export class DashboardClient {
  private vm: ViewModel = initialViewModel();
  private ws: WebSocket | null = null;
  private backoff: number;
  private readonly opts: Required<ClientOptions>;
  private stopped = false;

  constructor(
    private readonly onChange: (vm: ViewModel) => void,
    options: ClientOptions = {},
  ) {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    this.opts = {
      url: options.url ?? `${proto}://${location.host}/events`,
      backoffInitialMs: options.backoffInitialMs ?? 1000,
      backoffMaxMs: options.backoffMaxMs ?? 15000,
    };
    this.backoff = this.opts.backoffInitialMs;
  }

  start(): void {
    this.stopped = false;
    this.connect();
  }

  stop(): void {
    this.stopped = true;
    this.ws?.close();
  }

  private connect(): void {
    if (this.stopped) return;
    const ws = new WebSocket(this.opts.url);
    this.ws = ws;
    ws.onmessage = (msg: MessageEvent<string>) => {
      const frame = JSON.parse(msg.data) as WsFrame;
      if (frame.kind === "snapshot") {
        this.backoff = this.opts.backoffInitialMs;
        this.vm = applySnapshot(this.vm, frame.state as Snapshot);
      } else {
        this.vm = applyEvent(this.vm, frame.event);
        if (this.vm.connection === "stale") {
          ws.close(); // stream.gap: force a re-snapshot via reconnect
        }
      }
      this.onChange(this.vm);
    };
    ws.onclose = () => {
      if (this.stopped) return;
      this.vm.connection = "stale";
      this.onChange(this.vm);
      setTimeout(() => this.connect(), this.backoff);
      this.backoff = Math.min(this.backoff * 2, this.opts.backoffMaxMs);
    };
    ws.onerror = () => ws.close();
  }
}
