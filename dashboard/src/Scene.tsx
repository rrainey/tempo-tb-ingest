/**
 * The diorama (step 18, per docs/dashboard-notes.md): sky band, RSSI tiers,
 * device cards, base system box, and the accent bit-stream transfer
 * animation. Pure presentation over the view-model; geometry from layout.ts.
 */

import type { ViewModel } from "./reducer";
import { awaySeconds, type DeviceView } from "./reducer";
import {
  BASE,
  CARD_H,
  CARD_W,
  layoutDevices,
  skyLinePath,
  transferPath,
  VIEW_H,
  VIEW_W,
  type Placement,
} from "./layout";
import { dashCycleSeconds, font, palette } from "./palette";

function DeviceCard({ dev, at, now }: { dev: DeviceView; at: Placement; now: string }) {
  const sky = dev.zone === "sky";
  const active = dev.transfer !== null;
  const stroke = dev.flags.unprovisioned
    ? palette.warn
    : active
      ? palette.accent
      : sky
        ? palette.dim
        : palette.mid;
  const away = sky ? awaySeconds(dev, now) : null;
  return (
    <g
      transform={`translate(${at.x}, ${at.y})`}
      style={{ transition: "transform 900ms cubic-bezier(.4,0,.2,1)" }}
    >
      <rect
        width={CARD_W}
        height={CARD_H}
        rx={14}
        fill={sky ? palette.skyCardFill : palette.cardFill}
        stroke={stroke}
        strokeWidth={active ? 2.5 : 1.5}
        strokeDasharray={dev.flags.unprovisioned ? "7 5" : undefined}
        opacity={sky ? 0.8 : 1}
      />
      <text x={CARD_W / 2} y={32} textAnchor="middle" fill={palette.bright} fontSize={24} fontFamily={font}>
        {dev.label}
      </text>
      <text x={CARD_W / 2} y={56} textAnchor="middle" fill={palette.mid} fontSize={15} fontFamily={font}>
        {dev.flags.unprovisioned ? "needs setup" : (dev.jumper ?? "—") + (dev.isLo ? " ◆" : "")}
      </text>
      {dev.badge > 0 && (
        <g transform={`translate(${CARD_W - 26}, 4)`}>
          <rect width={30} height={20} rx={10} x={-6} fill={palette.dim} />
          <text x={9} y={14} textAnchor="middle" fill={palette.brightest} fontSize={12} fontFamily={font}>
            ✓{dev.badge}
          </text>
        </g>
      )}
      {dev.pendingDownload > 0 && (
        <text x={8} y={CARD_H - 8} fill={palette.bright} fontSize={13} fontFamily={font}>
          ▼{dev.pendingDownload}
        </text>
      )}
      {(dev.flags.conflicted || dev.flags.truncated || dev.flags.unprovisioned) && (
        <text x={CARD_W - 8} y={CARD_H - 8} textAnchor="end" fill={palette.warn} fontSize={14} fontFamily={font}>
          {dev.flags.unprovisioned ? "!!" : dev.flags.conflicted ? "‼id" : "⚠"}
        </text>
      )}
      {away != null && (
        <text x={CARD_W / 2} y={CARD_H + 22} textAnchor="middle" fill={palette.dim} fontSize={15} fontFamily={font}>
          ↑ {Math.floor(away / 60)}m {Math.floor(away % 60)}s
        </text>
      )}
    </g>
  );
}

function TransferBeam({ dev, at }: { dev: DeviceView; at: Placement }) {
  const t = dev.transfer!;
  const path = transferPath(at);
  const cycle = dashCycleSeconds(t.rateBps);
  const midX = (at.x + CARD_W / 2 + VIEW_W / 2) / 2;
  const midY = (at.y + CARD_H + BASE.y) / 2;
  return (
    <g>
      <path d={path} fill="none" stroke={palette.accent} strokeOpacity={0.25} strokeWidth={6} />
      <path
        d={path}
        fill="none"
        stroke={palette.accent}
        strokeWidth={3}
        strokeDasharray="4 18"
        strokeLinecap="round"
        markerEnd="url(#arrow-accent)"
        style={{ animation: `bit-flow ${cycle}s linear infinite` }}
      />
      <text x={midX + 16} y={midY} fill={palette.accent} fontSize={15} fontFamily={font}>
        {(t.bytesDone / 1024).toFixed(0)} KB · {(t.rateBps / 1024).toFixed(1)} KB/s
        {t.fileTotal > 1 ? ` · file ${t.fileIndex}/${t.fileTotal}` : ""}
      </text>
    </g>
  );
}

export default function Scene({ vm, now }: { vm: ViewModel; now: string }) {
  const devices = [...vm.devices.values()];
  const placed = layoutDevices(devices);
  const busy = devices.filter((d) => d.transfer !== null);

  return (
    <svg
      viewBox={`0 0 ${VIEW_W} ${VIEW_H}`}
      preserveAspectRatio="xMidYMid meet"
      style={{ width: "100%", height: "100%", display: "block", background: palette.bg }}
    >
      <defs>
        <marker id="arrow-accent" viewBox="0 0 10 10" refX={8} refY={5} markerWidth={7} markerHeight={7} orient="auto-start-reverse">
          <path d="M 0 0 L 10 5 L 0 10 z" fill={palette.accent} />
        </marker>
      </defs>

      {/* sky separator */}
      <path d={skyLinePath()} fill="none" stroke={palette.dim} strokeWidth={3} strokeDasharray="18 14" />
      <text x={VIEW_W - 24} y={40} textAnchor="end" fill={palette.dim} fontSize={15} fontFamily={font}>
        in the sky
      </text>

      {/* transfer beams under the cards */}
      {busy.map((d) => {
        const at = placed.get(d.id);
        return at ? <TransferBeam key={`beam-${d.id}`} dev={d} at={at} /> : null;
      })}

      {/* base system */}
      <rect
        x={BASE.x}
        y={BASE.y}
        width={BASE.w}
        height={BASE.h}
        rx={16}
        fill={palette.bgPanel}
        stroke={busy.length > 0 ? palette.accent : palette.mid}
        strokeWidth={busy.length > 0 ? 2.5 : 1.5}
      />
      <text x={VIEW_W / 2} y={BASE.y + 38} textAnchor="middle" fill={palette.bright} fontSize={20} fontFamily={font}>
        base system
      </text>
      <text
        x={VIEW_W / 2}
        y={BASE.y + 66}
        textAnchor="middle"
        fill={busy.length ? palette.accent : palette.mid}
        fontSize={14}
        fontFamily={font}
        style={
          vm.scanning && busy.length === 0
            ? { animation: "pulse 2.5s ease-in-out infinite" }
            : undefined
        }
      >
        {busy.length > 0 ? `receiving from ${busy[0].label}` : vm.scanning ? "● scanning" : "○ idle"}
      </text>

      {/* device cards */}
      {devices.map((d) => {
        const at = placed.get(d.id);
        return at ? <DeviceCard key={d.id} dev={d} at={at} now={now} /> : null;
      })}
    </svg>
  );
}
