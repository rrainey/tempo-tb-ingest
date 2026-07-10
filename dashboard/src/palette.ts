/**
 * Visual constants (step 18). Monochrome green family on near-black, with a
 * single amber accent reserved exclusively for active data transfer
 * (docs/dashboard-notes.md decisions 1 & 6). All provisional values are
 * centralized here for easy veto.
 */

export const palette = {
  bg: "#0b100b",
  bgPanel: "#101a10",
  grid: "#1c2b1c",
  dim: "#2e5230", // structure lines, sky line, idle strokes
  mid: "#4f8f52", // secondary text, card strokes
  bright: "#8fe08f", // primary text, active card strokes
  brightest: "#c8ffc8", // emphasis
  cardFill: "#12200f",
  skyCardFill: "#0e180e",
  accent: "#e8b23c", // ACTIVE TRANSFER ONLY
  warn: "#e8b23c",
  danger: "#e05d5d", // reserved: stale overlay text only
} as const;

export const font = "ui-monospace, 'Cascadia Code', Menlo, monospace";

/** Dash-cycle duration for the transfer bit stream: faster = higher rate.
 *  ~40 KB/s (typical) → ~0.8 s; clamped so it never freezes or blurs. */
export function dashCycleSeconds(rateBps: number): number {
  if (rateBps <= 0) return 2.5;
  return Math.min(2.5, Math.max(0.25, 32768 / rateBps));
}
