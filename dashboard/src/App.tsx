/**
 * Step-17 minimal state view: proves the pipe (roster + ticker + totals).
 * The step-18 visualization (sky line, tiers, bit-stream animation — see
 * docs/dashboard-notes.md) replaces this markup, keeping the same view-model.
 */

import { useEffect, useRef, useState } from "react";
import { DashboardClient } from "./client";
import { awaySeconds, initialViewModel, type DeviceView, type ViewModel } from "./reducer";

function DeviceRow({ dev, now }: { dev: DeviceView; now: string }) {
  const away = awaySeconds(dev, now);
  return (
    <tr>
      <td>{dev.label}</td>
      <td>{dev.jumper ?? "—"}{dev.isLo ? " (LO)" : ""}</td>
      <td>
        {dev.zone === "sky"
          ? `in the sky${away != null ? ` ↑${Math.floor(away / 60)}m` : ""}`
          : `tier ${dev.tier ?? "?"}`}
      </td>
      <td>{dev.rssiSmoothed != null ? `${dev.rssiSmoothed.toFixed(0)} dBm` : ""}</td>
      <td>{dev.badge > 0 ? `✓${dev.badge}` : ""}</td>
      <td>{dev.pendingDownload > 0 ? `▼${dev.pendingDownload}` : ""}</td>
      <td>
        {dev.transfer
          ? `${(dev.transfer.bytesDone / 1024).toFixed(0)} KB @ ${(dev.transfer.rateBps / 1024).toFixed(1)} KB/s`
          : ""}
      </td>
      <td>
        {dev.flags.unprovisioned ? "!! " : ""}
        {dev.flags.conflicted ? "CONFLICT " : ""}
        {dev.flags.truncated ? "TRUNC " : ""}
      </td>
    </tr>
  );
}

export default function App() {
  const [vm, setVm] = useState<ViewModel>(initialViewModel);
  const [now, setNow] = useState(() => new Date().toISOString());
  const clientRef = useRef<DashboardClient | null>(null);

  useEffect(() => {
    const client = new DashboardClient((next) => setVm({ ...next }));
    clientRef.current = client;
    client.start();
    const timer = setInterval(() => setNow(new Date().toISOString()), 1000);
    return () => {
      client.stop();
      clearInterval(timer);
    };
  }, []);

  const devices = [...vm.devices.values()].sort((a, b) => a.label.localeCompare(b.label));
  return (
    <div style={{ padding: 16 }}>
      <h2>
        tempo-tb-ingest {vm.daemonVersion}{" "}
        <small>
          [{vm.connection}
          {vm.scanning ? " · scanning" : ""}]
        </small>
      </h2>
      {vm.connection === "stale" && (
        <div style={{ color: "#e0c040" }}>⚠ RECONNECTING — display may be stale</div>
      )}
      {vm.warnings.map((w) => (
        <div key={w} style={{ color: "#e0c040" }}>⚠ {w}</div>
      ))}
      <table cellPadding={6}>
        <thead>
          <tr>
            <th>id</th><th>jumper</th><th>zone</th><th>rssi</th>
            <th>collected</th><th>pending</th><th>transfer</th><th>flags</th>
          </tr>
        </thead>
        <tbody>
          {devices.map((d) => (
            <DeviceRow key={d.id} dev={d} now={now} />
          ))}
        </tbody>
      </table>
      <p>
        stored {vm.totals.sessions_stored} · {(vm.totals.bytes_stored / (1 << 20)).toFixed(1)} MB ·
        pending {vm.totals.pending_download ?? 0} · harvests {vm.totals.harvests_completed} ·
        errors {vm.totals.failures}
      </p>
      <ul>
        {vm.ticker.map((t, i) => (
          <li key={`${t.ts}-${i}`}>
            {t.ts.slice(11, 19)} {t.text}
          </li>
        ))}
      </ul>
    </div>
  );
}
