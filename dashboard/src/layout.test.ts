import { describe, expect, it } from "vitest";
import { dashCycleSeconds } from "./palette";
import {
  BASE,
  CARD_W,
  layoutDevices,
  SKY_LINE_Y,
  SKY_Y,
  TIER_Y,
  transferPath,
  VIEW_W,
} from "./layout";
import type { DeviceView } from "./reducer";

function dev(id: string, zone: "sky" | "tiers", tier: 1 | 2 | 3 | null = 1): DeviceView {
  return {
    id,
    label: id,
    jumper: null,
    isLo: false,
    zone,
    tier,
    rssiSmoothed: -60,
    awaySince: null,
    badge: 0,
    pendingDownload: 0,
    transfer: null,
    flags: { unprovisioned: false, conflicted: false, truncated: false },
    lastSeen: null,
  };
}

describe("layoutDevices", () => {
  it("separates zones vertically (sky above the line, tier 1 lowest)", () => {
    const placed = layoutDevices([dev("A", "sky", null), dev("B", "tiers", 3), dev("C", "tiers", 1)]);
    expect(placed.get("A")!.y).toBe(SKY_Y);
    expect(placed.get("A")!.y).toBeLessThan(SKY_LINE_Y);
    expect(placed.get("B")!.y).toBe(TIER_Y[3]);
    expect(placed.get("C")!.y).toBe(TIER_Y[1]);
    expect(placed.get("C")!.y).toBeGreaterThan(placed.get("B")!.y);
    expect(placed.get("C")!.y).toBeLessThan(BASE.y);
  });

  it("never overlaps cards within a crowded tier", () => {
    const many = Array.from({ length: 8 }, (_, i) => dev(`000${i}`, "tiers", 2));
    const placed = layoutDevices(many);
    const xs = [...placed.values()].map((p) => p.x).sort((a, b) => a - b);
    for (let i = 1; i < xs.length; i++) {
      expect(xs[i] - xs[i - 1]).toBeGreaterThanOrEqual(CARD_W * 0.9);
    }
    expect(xs[0]).toBeGreaterThanOrEqual(0);
    expect(xs[xs.length - 1] + CARD_W).toBeLessThanOrEqual(VIEW_W);
  });

  it("is deterministic and label-ordered", () => {
    const a = layoutDevices([dev("0002", "tiers", 1), dev("0001", "tiers", 1)]);
    const b = layoutDevices([dev("0001", "tiers", 1), dev("0002", "tiers", 1)]);
    expect(a.get("0001")).toEqual(b.get("0001"));
    expect(a.get("0001")!.x).toBeLessThan(a.get("0002")!.x);
  });
});

describe("transferPath", () => {
  it("starts at the card bottom-center and ends at the base", () => {
    const path = transferPath({ x: 100, y: 550 });
    expect(path.startsWith(`M ${100 + CARD_W / 2} ${550 + 84}`)).toBe(true);
    expect(path.endsWith(`${VIEW_W / 2} ${BASE.y}`)).toBe(true);
  });
});

describe("dashCycleSeconds", () => {
  it("maps typical rates sensibly and clamps extremes", () => {
    expect(dashCycleSeconds(40960)).toBeCloseTo(0.8, 1); // ~40 KB/s
    expect(dashCycleSeconds(0)).toBe(2.5); // stalled: slow crawl
    expect(dashCycleSeconds(10)).toBe(2.5); // clamp floor speed
    expect(dashCycleSeconds(10_000_000)).toBe(0.25); // clamp ceiling
  });
});
