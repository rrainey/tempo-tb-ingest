/**
 * Step 17: reducer/view-model tests, driven by the same fixture recordings
 * the Python suite locks (../tests/fixtures) — one contract, two consumers.
 */

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import type { Envelope, Snapshot } from "./contract";
import {
  applyEvent,
  applySnapshot,
  awaySeconds,
  initialViewModel,
  tierFor,
  type ViewModel,
} from "./reducer";

const fixturesDir = fileURLToPath(new URL("../../tests/fixtures/", import.meta.url));

function loadRecording(name: string): Envelope[] {
  return readFileSync(fixturesDir + name, "utf-8")
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line) as Envelope);
}

function foldAll(events: Envelope[]): ViewModel {
  let vm = initialViewModel();
  vm.connection = "live";
  for (const env of events) vm = applyEvent(vm, env);
  return vm;
}

describe("tierFor", () => {
  it("maps plain thresholds without history", () => {
    expect(tierFor(-50, null)).toBe(1);
    expect(tierFor(-70, null)).toBe(2);
    expect(tierFor(-90, null)).toBe(3);
  });

  it("applies hysteresis at the boundary", () => {
    // sitting in tier 2 at -61; a drift to -59 is within the 4 dBm margin
    expect(tierFor(-59, 2)).toBe(2);
    // a real approach to -55 crosses with margin: promote
    expect(tierFor(-55, 2)).toBe(1);
    // and the reverse: tier 1 at -62 holds, -65 demotes
    expect(tierFor(-62, 1)).toBe(1);
    expect(tierFor(-65, 1)).toBe(2);
  });
});

describe("synthetic-day recording", () => {
  const events = loadRecording("synthetic-day.jsonl");

  it("tells the whole story", () => {
    const vm = foldAll(events);
    expect(vm.totals.sessions_stored).toBe(3);
    expect(vm.totals.harvests_completed).toBe(2);
    expect(vm.totals.failures).toBe(1);
    expect(vm.totals.pending_download).toBe(0);
    const dev = vm.devices.get("0001")!;
    expect(dev.badge).toBe(3);
    expect(dev.zone).toBe("tiers");
    expect(dev.transfer).toBeNull();
    expect(vm.ticker.length).toBeGreaterThan(0);
    expect(vm.scanning).toBe(false); // daemon.stopping ends the recording
  });

  it("tracks the away/return cycle with timer material", () => {
    let vm = initialViewModel();
    vm.connection = "live";
    const untilAway = events.slice(
      0,
      events.findIndex((e) => e.type === "device.away") + 1,
    );
    for (const env of untilAway) vm = applyEvent(vm, env);
    const dev = vm.devices.get("0001")!;
    expect(dev.zone).toBe("sky");
    expect(dev.awaySince).not.toBeNull();
    const later = new Date(Date.parse(dev.awaySince!) + 300_000).toISOString();
    expect(awaySeconds(dev, later)).toBeCloseTo(300, 0);
  });

  it("shows pending_download during the failed harvest window", () => {
    let vm = initialViewModel();
    vm.connection = "live";
    const untilFailed = events.slice(
      0,
      events.findIndex((e) => e.type === "harvest.failed") + 1,
    );
    for (const env of untilFailed) vm = applyEvent(vm, env);
    expect(vm.devices.get("0001")!.pendingDownload).toBe(1);
    expect(vm.totals.pending_download).toBe(1);
  });
});

describe("loop-replay restart boundary", () => {
  it("daemon.started resets run state but keeps the ticker", () => {
    const events = loadRecording("live-harvest-20260708.jsonl");
    let vm = foldAll(events);
    expect(vm.totals.sessions_stored).toBe(13);
    expect(vm.ticker.length).toBeGreaterThan(0);
    const tickerBefore = vm.ticker.length;
    vm = applyEvent(vm, {
      v: 1,
      seq: vm.lastSeq + 1,
      ts: "2026-07-10T12:00:00.000Z",
      type: "daemon.started",
      data: { version: "replay-loop", config: {} },
    });
    expect(vm.totals.sessions_stored).toBe(0);
    expect(vm.devices.size).toBe(0);
    expect(vm.ticker.length).toBe(tickerBefore); // history survives the loop
    // and a re-run of the recording (re-sequenced) animates again
    const offset = vm.lastSeq;
    for (const env of events) {
      vm = applyEvent(vm, { ...env, seq: env.seq + offset });
    }
    expect(vm.totals.sessions_stored).toBe(13);
  });
});

describe("live harvest recording (real hardware, 2026-07-08)", () => {
  const events = loadRecording("live-harvest-20260708.jsonl");

  it("reproduces the validated harvest results", () => {
    const vm = foldAll(events);
    expect(vm.totals.sessions_stored).toBe(13);
    expect(vm.totals.harvests_completed).toBe(2);
    expect(vm.totals.failures).toBe(0);
    expect(vm.devices.get("0001")!.badge).toBe(11);
    expect(vm.devices.get("0007")!.badge).toBe(2);
  });
});

describe("snapshot handling", () => {
  const snapshot: Snapshot = {
    v: 1,
    seq: 100,
    ts: "2026-07-09T12:00:00.000Z",
    daemon: {
      version: "0.1.0",
      started_at: null,
      adapters: { scan: "hci0", transfer: ["hci0"] },
      scanning: true,
      warnings: ["session list truncated on 0002"],
    },
    devices: [
      {
        id: "0001",
        name: "Tempo-BT-0001",
        mac: "AA",
        jumper: "riley",
        is_lo: true,
        state: "PRESENT",
        rssi: -58,
        last_seen: "2026-07-09T11:59:59.000Z",
        away_since: null,
        sessions_known: 26,
        pending_download: 0,
      },
      {
        id: null,
        name: "Tempo-BT",
        mac: "F5:23",
        provisioning_needed: true,
      },
    ],
    queue: [],
    active_job: null,
    totals: {
      sessions_stored: 29,
      bytes_stored: 1000,
      pending_download: 0,
      harvests_completed: 7,
      failures: 1,
    },
  };

  it("builds the roster including unprovisioned devices", () => {
    const vm = applySnapshot(initialViewModel(), snapshot);
    expect(vm.connection).toBe("live");
    expect(vm.devices.get("0001")!.jumper).toBe("riley");
    expect(vm.devices.get("0001")!.tier).toBe(1);
    const up = vm.devices.get("mac:F5:23")!;
    expect(up.flags.unprovisioned).toBe(true);
    expect(up.label).toBe("!!");
    expect(vm.warnings).toContain("session list truncated on 0002");
  });

  it("drops events already reflected in the snapshot", () => {
    let vm = applySnapshot(initialViewModel(), snapshot);
    const stale: Envelope = {
      v: 1,
      seq: 99,
      ts: "2026-07-09T11:58:00.000Z",
      type: "store.session_added",
      data: { id: "0001", session_key: "x/y", path: "p", size: 5, sha256: "0", jumper: null },
    };
    vm = applyEvent(vm, stale);
    expect(vm.totals.sessions_stored).toBe(29); // unchanged
  });

  it("stream.gap marks the view stale", () => {
    let vm = applySnapshot(initialViewModel(), snapshot);
    vm = applyEvent(vm, {
      v: 1,
      seq: -1,
      ts: "2026-07-09T12:00:01.000Z",
      type: "stream.gap",
      data: { dropped_count: 5 },
    });
    expect(vm.connection).toBe("stale");
  });

  it("re-snapshot preserves the ticker across reconnects", () => {
    let vm = applySnapshot(initialViewModel(), snapshot);
    vm = applyEvent(vm, {
      v: 1,
      seq: 101,
      ts: "2026-07-09T12:00:02.000Z",
      type: "harvest.completed",
      data: { id: "0001", sessions_downloaded: 2, bytes: 2048, duration_s: 10 },
    });
    expect(vm.ticker.length).toBe(1);
    const after = applySnapshot(vm, { ...snapshot, seq: 102 });
    expect(after.ticker.length).toBe(1);
  });
});
