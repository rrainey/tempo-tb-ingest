/**
 * Step-18 dashboard: the full-screen diorama (Scene) + HTML overlay chrome —
 * header, stats (lower-right), event log (lower-left), legend popup, and the
 * unmissable stale treatment. Layout decisions: docs/dashboard-notes.md.
 */

import { useEffect, useState } from "react";
import { DashboardClient } from "./client";
import Scene from "./Scene";
import { initialViewModel, type ViewModel } from "./reducer";
import { palette } from "./palette";

const LEGEND: [string, string][] = [
  ["— above dashed line —", "device away: presumed on a jump (↑ time away)"],
  ["three rows", "signal tiers; lowest row = strongest reception"],
  ["amber moving bits", "active download, device → base (speed ∝ data rate)"],
  ["✓N", "sessions collected this run"],
  ["▼N", "sessions discovered on device, not yet downloaded"],
  ["◆ after name", "load organizer (default formation base)"],
  ["dashed card + !!", "unprovisioned device — needs a name (never harvested)"],
  ["‼id", "identity conflict: duplicate device suffix in the fleet"],
  ["⚠", "session list truncated on device — archive the card"],
];

function Stats({ vm }: { vm: ViewModel }) {
  const collectedRun = [...vm.devices.values()].reduce((n, d) => n + d.badge, 0);
  const rate = [...vm.devices.values()].find((d) => d.transfer)?.transfer?.rateBps ?? 0;
  const rows: [string, string][] = [
    ["devices in use", String(vm.devices.size)],
    ["collected (run)", String(collectedRun)],
    ["stored (all time)", String(vm.totals.sessions_stored)],
    ["pending download", String(vm.totals.pending_download ?? 0)],
    ["data rate", rate > 0 ? `${(rate / 1024).toFixed(1)} KB/s` : "—"],
    ["errors", String(vm.totals.failures)],
  ];
  return (
    <div className="overlay panel" style={{ right: 18, bottom: 18, minWidth: 250 }}>
      <div style={{ color: palette.bright, marginBottom: 6 }}>status</div>
      <table style={{ borderSpacing: 0 }}>
        <tbody>
          {rows.map(([k, v]) => (
            <tr key={k}>
              <td style={{ color: palette.mid, paddingRight: 16 }}>{k}</td>
              <td style={{ color: palette.brightest, textAlign: "right" }}>{v}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {vm.warnings.map((w) => (
        <div key={w} style={{ color: palette.warn, marginTop: 6, maxWidth: 300 }}>
          ⚠ {w}
        </div>
      ))}
    </div>
  );
}

function Ticker({ vm }: { vm: ViewModel }) {
  if (vm.ticker.length === 0) return null;
  return (
    <div className="overlay panel" style={{ left: 18, bottom: 18, minWidth: 300 }}>
      {vm.ticker.map((t, i) => (
        <div
          key={`${t.ts}-${i}`}
          className="ticker-entry"
          style={{ color: palette.bright, opacity: Math.max(0.25, 1 - i * 0.15) }}
        >
          <span style={{ color: palette.dim }}>{t.ts.slice(11, 19)} </span>
          {t.text}
        </div>
      ))}
    </div>
  );
}

function Legend({ onClose }: { onClose: () => void }) {
  return (
    <div className="stale-veil" style={{ background: "rgba(4,6,4,0.85)" }} onClick={onClose}>
      <div className="panel" style={{ maxWidth: 620 }}>
        <div style={{ color: palette.bright, marginBottom: 10 }}>legend</div>
        <table style={{ borderSpacing: "0 4px" }}>
          <tbody>
            {LEGEND.map(([symbol, meaning]) => (
              <tr key={symbol}>
                <td style={{ color: palette.brightest, paddingRight: 18, whiteSpace: "nowrap" }}>
                  {symbol}
                </td>
                <td style={{ color: palette.mid }}>{meaning}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <div style={{ color: palette.dim, marginTop: 10 }}>click anywhere to close</div>
      </div>
    </div>
  );
}

export default function App() {
  const [vm, setVm] = useState<ViewModel>(initialViewModel);
  const [now, setNow] = useState(() => new Date().toISOString());
  const [legendOpen, setLegendOpen] = useState(false);

  useEffect(() => {
    const client = new DashboardClient((next) => setVm({ ...next }));
    client.start();
    const timer = setInterval(() => setNow(new Date().toISOString()), 1000);
    return () => {
      client.stop();
      clearInterval(timer);
    };
  }, []);

  const clock = new Date(now);
  return (
    <div style={{ position: "relative", height: "100%", overflow: "hidden" }}>
      <Scene vm={vm} now={now} />

      <div className="overlay" style={{ left: 18, top: 14, color: palette.mid }}>
        <span style={{ color: palette.bright, letterSpacing: 2 }}>TEMPO INGEST</span>
        <span> {vm.daemonVersion}</span>
        <span style={{ color: vm.connection === "live" ? palette.bright : palette.warn }}>
          {" "}
          · {vm.connection}
        </span>
      </div>

      <div className="overlay" style={{ right: 18, top: 14, display: "flex", gap: 14, alignItems: "center" }}>
        <span style={{ color: palette.mid }}>
          {clock.toLocaleTimeString([], { hour12: false })}{" "}
          <span style={{ color: palette.dim }}>
            / {clock.toISOString().slice(11, 19)}Z
          </span>
        </span>
        <button className="legend-button" onClick={() => setLegendOpen(true)} title="legend">
          ?
        </button>
      </div>

      <Ticker vm={vm} />
      <Stats vm={vm} />

      {legendOpen && <Legend onClose={() => setLegendOpen(false)} />}

      {vm.connection !== "live" && (
        <div className="stale-veil">
          <div style={{ textAlign: "center" }}>
            <div style={{ color: palette.danger, fontSize: 34, letterSpacing: 6 }}>
              {vm.connection === "stale" ? "RECONNECTING" : "CONNECTING"}
            </div>
            <div style={{ color: palette.mid, marginTop: 8 }}>display may be stale</div>
          </div>
        </div>
      )}
    </div>
  );
}
