/**
 * TypeScript mirror of the daemon↔dashboard wire contract (design.md §6).
 * Additive changes only within v:1; keep in sync with the golden fixtures in
 * ../tests/fixtures (Python side locks the same shapes).
 */

export interface Envelope {
  v: number;
  seq: number;
  ts: string; // ISO-8601 UTC, millisecond Z
  type: string;
  data: Record<string, unknown>;
}

export type WsFrame =
  | { kind: "snapshot"; state: Snapshot }
  | { kind: "event"; event: Envelope };

export interface DeviceSnapshot {
  id: string | null; // null = unprovisioned (bare "Tempo-BT")
  name: string | null;
  folder?: string;
  mac: string | null;
  jumper?: string | null;
  is_lo?: boolean;
  state?: "PRESENT" | "AWAY";
  rssi?: number | null;
  last_seen?: string | null;
  away_since?: string | null;
  sessions_known?: number;
  pending_download?: number;
  provisioning_needed?: boolean;
  conflicted?: boolean;
  truncated?: boolean;
}

export interface ActiveJob {
  id: string;
  state: string;
  session_key: string | null;
  file_index: number | null;
  file_total: number | null;
  bytes_done: number;
  bytes_total: number | null;
  rate_bps: number;
}

export interface Totals {
  sessions_stored: number;
  bytes_stored: number;
  pending_download?: number;
  harvests_completed: number;
  failures: number;
}

export interface Snapshot {
  v: number;
  seq: number;
  ts: string | null;
  daemon: {
    version: string;
    started_at: string | null;
    adapters: { scan: string | null; transfer: string[] };
    scanning: boolean;
    warnings: string[];
  };
  devices: DeviceSnapshot[];
  queue: { id: string; queued_at: string }[];
  active_job: ActiveJob | null;
  totals: Totals;
}
