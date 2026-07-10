/**
 * Pure scene geometry (step 18): a 1600×900 viewBox diorama.
 * Regions per docs/dashboard-notes.md: sky band above a dashed sky line,
 * three RSSI tier bands (tier 1 = strongest = closest to the base), the base
 * system box bottom-center. Pure functions — unit-tested.
 */

import type { DeviceView } from "./reducer";

export const VIEW_W = 1600;
export const VIEW_H = 900;

export const CARD_W = 150;
export const CARD_H = 84;

export const SKY_Y = 70;
export const SKY_LINE_Y = 215;
export const TIER_Y: Record<1 | 2 | 3, number> = { 3: 270, 2: 410, 1: 550 };

export const BASE = { x: VIEW_W / 2 - 240, y: 730, w: 480, h: 90 } as const;

const MARGIN_X = 90;

export interface Placement {
  x: number; // card top-left
  y: number;
}

function slotRow(count: number, index: number): number {
  const usable = VIEW_W - 2 * MARGIN_X;
  const step = usable / count;
  return MARGIN_X + step * (index + 0.5) - CARD_W / 2;
}

/** Deterministic, collision-free positions for every device card. */
export function layoutDevices(devices: DeviceView[]): Map<string, Placement> {
  const zones = new Map<string, DeviceView[]>();
  for (const dev of devices) {
    const zone = dev.zone === "sky" ? "sky" : `tier${dev.tier ?? 3}`;
    const group = zones.get(zone) ?? [];
    group.push(dev);
    zones.set(zone, group);
  }
  const out = new Map<string, Placement>();
  for (const [zone, group] of zones) {
    group.sort((a, b) => a.label.localeCompare(b.label));
    const y = zone === "sky" ? SKY_Y : TIER_Y[Number(zone.slice(4)) as 1 | 2 | 3];
    group.forEach((dev, i) => {
      out.set(dev.id, { x: slotRow(group.length, i), y });
    });
  }
  return out;
}

/** Curved transfer path from a device card to the base box. */
export function transferPath(from: Placement): string {
  const x1 = from.x + CARD_W / 2;
  const y1 = from.y + CARD_H;
  const x2 = VIEW_W / 2;
  const y2 = BASE.y;
  const cx = (x1 + x2) / 2;
  const cy = y1 + (y2 - y1) * 0.35;
  return `M ${x1} ${y1} Q ${cx} ${cy} ${x2} ${y2}`;
}

/** The dashed "in the sky" separator: a gentle sine across the width. */
export function skyLinePath(): string {
  const amplitude = 16;
  const segments = 8;
  const step = VIEW_W / segments;
  let d = `M 0 ${SKY_LINE_Y}`;
  for (let i = 0; i < segments; i++) {
    const x = i * step;
    const midY = SKY_LINE_Y + (i % 2 === 0 ? -amplitude : amplitude);
    d += ` Q ${x + step / 2} ${midY} ${x + step} ${SKY_LINE_Y}`;
  }
  return d;
}
