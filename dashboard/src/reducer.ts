/**
 * The view-model layer (step 17): folds snapshot + events into what the
 * visualization renders. Pure and synchronous — fully unit-testable; all
 * concept-driven derivations from docs/dashboard-notes.md live here:
 * smoothed RSSI tiers with hysteresis, away timers, transfer state, badges,
 * warnings, and the event ticker.
 */

import type { ActiveJob, Envelope, Snapshot, Totals } from "./contract";

// -- RSSI tiers (dashboard-notes): EMA ~5 s + ~4 dBm hysteresis -------------

export const TIER_BOUNDARIES_DBM = [-60, -75] as const; // tier1/tier2, tier2/tier3
export const TIER_HYSTERESIS_DBM = 4;
export const EMA_ALPHA = 0.3; // at ~1 sighting/s ≈ 5 s time constant

export type Tier = 1 | 2 | 3; // 1 = strongest (closest to base)

export function tierFor(rssiSmoothed: number, previous: Tier | null): Tier {
  const [b1, b2] = TIER_BOUNDARIES_DBM;
  const h = TIER_HYSTERESIS_DBM;
  let tier: Tier = rssiSmoothed >= b1 ? 1 : rssiSmoothed >= b2 ? 2 : 3;
  if (previous !== null && tier !== previous) {
    // require crossing the boundary by the hysteresis margin
    const boundary = Math.min(previous, tier) === 1 ? b1 : b2;
    const margin = tier < previous ? rssiSmoothed - boundary : boundary - rssiSmoothed;
    if (margin < h) tier = previous;
  }
  return tier;
}

// -- view-model types --------------------------------------------------------

export interface DeviceView {
  id: string; // unprovisioned devices use "mac:<addr>" as the view key
  label: string; // "0009" or "!!" for unprovisioned
  jumper: string | null;
  isLo: boolean;
  zone: "sky" | "tiers";
  tier: Tier | null; // null while in the sky
  rssiSmoothed: number | null;
  awaySince: string | null; // ISO ts; render derives the live timer
  badge: number; // sessions collected this daemon run
  pendingDownload: number;
  transfer: null | { sessionKey: string; bytesDone: number; rateBps: number; fileIndex: number; fileTotal: number };
  flags: { unprovisioned: boolean; conflicted: boolean; truncated: boolean };
  lastSeen: string | null;
}

export interface TickerEntry {
  ts: string;
  text: string;
}

export interface ViewModel {
  connection: "connecting" | "live" | "stale";
  daemonVersion: string;
  scanning: boolean;
  warnings: string[];
  devices: Map<string, DeviceView>;
  activeJob: ActiveJob | null;
  totals: Totals;
  ticker: TickerEntry[]; // newest first, bounded
  lastSeq: number;
}

const TICKER_LIMIT = 6;

export function initialViewModel(): ViewModel {
  return {
    connection: "connecting",
    daemonVersion: "",
    scanning: false,
    warnings: [],
    devices: new Map(),
    activeJob: null,
    totals: {
      sessions_stored: 0,
      bytes_stored: 0,
      pending_download: 0,
      harvests_completed: 0,
      failures: 0,
    },
    ticker: [],
    lastSeq: 0,
  };
}

// -- snapshot ----------------------------------------------------------------

export function applySnapshot(vm: ViewModel, snapshot: Snapshot): ViewModel {
  const next = initialViewModel();
  next.connection = "live";
  next.daemonVersion = snapshot.daemon.version;
  next.scanning = snapshot.daemon.scanning;
  next.warnings = [...snapshot.daemon.warnings];
  next.activeJob = snapshot.active_job;
  next.totals = { ...snapshot.totals };
  next.lastSeq = snapshot.seq;
  next.ticker = vm.ticker; // survives re-snapshots (reconnects)
  for (const d of snapshot.devices) {
    const key = d.id ?? `mac:${d.mac ?? "?"}`;
    const away = d.state === "AWAY";
    next.devices.set(key, {
      id: key,
      label: d.id ?? "!!",
      jumper: d.jumper ?? null,
      isLo: d.is_lo ?? false,
      zone: away ? "sky" : "tiers",
      tier: away || d.rssi == null ? null : tierFor(d.rssi, null),
      rssiSmoothed: d.rssi ?? null,
      awaySince: d.away_since ?? null,
      badge: 0, // per-run collection count is event-derived
      pendingDownload: d.pending_download ?? 0,
      transfer: null,
      flags: {
        unprovisioned: d.provisioning_needed ?? false,
        conflicted: d.conflicted ?? false,
        truncated: d.truncated ?? false,
      },
      lastSeen: d.last_seen ?? null,
    });
  }
  return next;
}

// -- events -------------------------------------------------------------------

function device(vm: ViewModel, id: string): DeviceView {
  let d = vm.devices.get(id);
  if (!d) {
    d = {
      id,
      label: id,
      jumper: null,
      isLo: false,
      zone: "tiers",
      tier: null,
      rssiSmoothed: null,
      awaySince: null,
      badge: 0,
      pendingDownload: 0,
      transfer: null,
      flags: { unprovisioned: false, conflicted: false, truncated: false },
      lastSeen: null,
    };
    vm.devices.set(id, d);
  }
  return d;
}

function tick(vm: ViewModel, ts: string, text: string): void {
  vm.ticker.unshift({ ts, text });
  if (vm.ticker.length > TICKER_LIMIT) vm.ticker.length = TICKER_LIMIT;
}

function fmtBytes(n: number): string {
  return n >= 1 << 20 ? `${(n / (1 << 20)).toFixed(1)} MB` : `${(n / 1024).toFixed(0)} KB`;
}

/** Mutates and returns vm (callers re-render on reference change upstream). */
export function applyEvent(vm: ViewModel, env: Envelope): ViewModel {
  if (env.seq > 0 && env.seq <= vm.lastSeq) return vm; // pre-snapshot replay
  if (env.seq > 0) vm.lastSeq = env.seq;
  const d = env.data as Record<string, never> & Record<string, unknown>;
  const id = typeof d.id === "string" ? d.id : null;

  switch (env.type) {
    case "device.new":
    case "device.seen": {
      if (!id) break;
      const dev = device(vm, id);
      dev.zone = "tiers";
      dev.awaySince = null;
      dev.lastSeen = env.ts;
      const rssi = d.rssi as number;
      dev.rssiSmoothed =
        dev.rssiSmoothed == null ? rssi : dev.rssiSmoothed + EMA_ALPHA * (rssi - dev.rssiSmoothed);
      dev.tier = tierFor(dev.rssiSmoothed, dev.tier);
      if (env.type === "device.new") tick(vm, env.ts, `${id} appeared`);
      break;
    }
    case "device.away": {
      if (!id) break;
      const dev = device(vm, id);
      dev.zone = "sky";
      dev.tier = null;
      dev.awaySince = d.away_since as string;
      break;
    }
    case "device.returned": {
      if (!id) break;
      const dev = device(vm, id);
      dev.zone = "tiers";
      const absent = d.absent_for_s as number | null;
      if (absent != null) tick(vm, env.ts, `${id} returned (away ${Math.round(absent / 60)} m)`);
      dev.awaySince = null;
      break;
    }
    case "device.lost":
      if (id) vm.devices.delete(id);
      break;
    case "device.provisioning_needed": {
      const key = `mac:${d.mac as string}`;
      const dev = device(vm, key);
      dev.label = "!!";
      dev.flags.unprovisioned = true;
      break;
    }
    case "device.identity_conflict":
      if (id) device(vm, id).flags.conflicted = true;
      break;
    case "harvest.session_list":
      if (id) device(vm, id).pendingDownload = d.new_count as number;
      break;
    case "harvest.truncated":
      if (id) device(vm, id).flags.truncated = true;
      break;
    case "transfer.started":
      if (id)
        device(vm, id).transfer = {
          sessionKey: d.session_key as string,
          bytesDone: d.resumed_from as number,
          rateBps: 0,
          fileIndex: d.file_index as number,
          fileTotal: d.file_total as number,
        };
      break;
    case "transfer.progress": {
      if (!id) break;
      const t = device(vm, id).transfer;
      if (t && t.sessionKey === d.session_key) {
        t.bytesDone = d.bytes_done as number;
        t.rateBps = d.rate_bps as number;
      }
      break;
    }
    case "transfer.completed":
      if (id) device(vm, id).transfer = null;
      break;
    case "transfer.failed":
      if (id) device(vm, id).transfer = null;
      break;
    case "store.session_added": {
      if (!id) break;
      const dev = device(vm, id);
      dev.badge += 1;
      dev.pendingDownload = Math.max(0, dev.pendingDownload - 1);
      vm.totals.sessions_stored += 1;
      vm.totals.bytes_stored += d.size as number;
      break;
    }
    case "harvest.completed": {
      vm.totals.harvests_completed += 1;
      const n = d.sessions_downloaded as number;
      const bytes = d.bytes as number;
      if (id && n > 0) tick(vm, env.ts, `${id} ✓ ${n} session(s), ${fmtBytes(bytes)}`);
      if (id) device(vm, id).transfer = null;
      break;
    }
    case "harvest.failed": {
      vm.totals.failures += 1;
      if (id) {
        device(vm, id).transfer = null;
        tick(vm, env.ts, `${id} harvest failed (${d.will_retry ? "will retry" : "gave up"})`);
      }
      break;
    }
    case "scanner.degraded":
      vm.scanning = false;
      vm.warnings = [...vm.warnings, `scanner degraded: ${d.reason as string}`];
      break;
    case "scanner.recovered":
      vm.scanning = true;
      vm.warnings = vm.warnings.filter((w) => !w.startsWith("scanner degraded"));
      break;
    case "daemon.stopping":
      vm.scanning = false;
      break;
    case "stream.gap":
      vm.connection = "stale"; // client must re-snapshot
      break;
  }
  vm.totals.pending_download = [...vm.devices.values()].reduce(
    (sum, dev) => sum + dev.pendingDownload,
    0,
  );
  return vm;
}

/** Seconds a sky device has been away, for the away timer. */
export function awaySeconds(dev: DeviceView, nowIso: string): number | null {
  if (dev.awaySince == null) return null;
  return Math.max(0, (Date.parse(nowIso) - Date.parse(dev.awaySince)) / 1000);
}
