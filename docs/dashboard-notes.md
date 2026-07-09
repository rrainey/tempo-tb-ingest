# Dashboard UI — brainstorm notes

*2026-07-09. Raw material for the forthcoming dashboard visual-design document
(owner: Riley). Source: sketch `ingest-dashboard-brainstorm.png` + discussion.
The daemon/dashboard data contract is design.md §6; step 17 builds the data
layer to serve exactly what's below.*

## Core concept

A live diorama of the dropzone, dark background, tones of a single green used
almost exclusively. The vertical axis is both signal quality and narrative
altitude:

```
────────────────  "in the sky" (dashed, wavy line)  ────────────────
   [0001 riley ↑14m]   [0005 billy ↑6m]          ← AWAY devices + away timer
─────────────────────────────────────────────────────────────────────
   tier 3 (weak RSSI)      [0003 able]
   tier 2 (medium)                   [0002 baker]
   tier 1 (strong)   [0008 delta]        [0009 echo]
                      ║ animated bits ║
              [        base system        ]      Status ┐
   event log (lower-left?)                   stats panel┘ (lower-right)
```

- A device **returning from a jump** descends from the sky, settles downward
  through tiers as the jumper walks in, the base harvests it (animated bit
  stream), and it gains a jumps-collected badge. The whole story is driven by
  events the daemon already emits.
- The sky metaphor is semantically honest: firmware disables BLE while
  logging, so an absent device really is (presumed) on a jump; AWAY is the
  presence machine's literal state.

## Decisions (2026-07-09)

1. **Palette**: dark mode; monochrome green family; brightness/intensity is
   the in-family vocabulary (bright = active/strong, dim = idle/weak).
   **One accent color** (exact hue negotiable; sketch used a dark yellow)
   reserved exclusively for active data transfer.
2. **Sky region**: only devices *seen today* that are not currently visible.
   Each shows an **away timer** ("↑ 14 min"). No context menu on sky devices.
3. **Visible devices**: three RSSI tiers to start; bottom tier = strongest
   reception. Placement within a tier dynamically assigned.
   - Anti-flap: EMA-smooth RSSI (~5 s) + tier hysteresis (~4 dBm past the
     boundary to change tier) — bench-observed jitter is ±5–10 dBm at rest.
4. **Device card**: rounded rectangle (echoes the enclosure shape); shows
   device index (e.g. `0009`) + jumper name (from device-owners.json — the
   daemon snapshot resolves names from the registry so labels are immediate,
   not harvest-dependent).
   - **Jumps-collected badge** (e.g. "✓ 2") after successful harvests.
   - Kebab (⋮) context-menu button — **future**: reboot, identify (LED
     blink), rename… Implies daemon control endpoints (v2; v1 is read-only).
5. **Unprovisioned devices** (bare `Tempo-BT`): uncommon; dashed outline +
   needs-attention symbol ("!!"). No dedicated zone — they appear among the
   tiers like any sighted device.
6. **Transfer animation**: a line of moving "bits" between device and base;
   arrowheads show current direction; bit speed/density proportional to
   `rate_bps` (the animation *is* the throughput meter). Uses the accent
   color. (Direction is almost always device→base in v1 — downloads.)
7. **Stats panel — lower right**. Fields (terminology: **download** = device→
   base; upload was a misnomer):
   - Devices in use (seen today)
   - Downloaded today (sessions)
   - **Pending download** — files discovered on devices, not yet downloaded
   - Data rate (active transfer)
   - Errors
8. **Event/history log**: a short fading list of notable events —
   "0007 scott_z — 2 sessions, 4.4 MB ✓", "0001 riley returned (away 22 m)".
   Position: upper-right or lower-left, TBD (lower-left balances the
   lower-right stats panel).
9. **Warning glyphs** on the affected element (device card or base box):
   session-list truncated, identity conflict, unprovisioned, scanner
   degraded. A **Help/Legend button** opens a popup explaining every glyph
   and visual convention.
10. **Stream-stale indicator** must be unmissable (whole-display dim +
    "RECONNECTING" watermark) — a frozen dashboard that looks alive is the
    worst failure mode. (Design §7 requirement.)
11. **Base system box**: bottom center; can carry daemon-level state
    (scanning indicator; scanner pauses during a connection — the base
    visibly "focuses" on one device at a time, which is truthful: BlueZ
    cannot scan and connect simultaneously).

## Event → visual mapping

| Event | Visual |
|---|---|
| `device.new` / `device.seen` | card appears / RSSI tier updates (smoothed) |
| `device.away` | card floats above the sky line; away timer starts |
| `device.returned` | descends from sky (absent_for shown briefly) |
| `device.lost` | card fades out |
| `harvest.queued/started` | card highlights; base focuses |
| `transfer.started/progress` | accent bit-stream line; speed ∝ rate_bps |
| `transfer.completed` / `store.session_added` | pulse on card; badge count++ |
| `harvest.completed` | settle animation; event-log line |
| `harvest.failed` / `store.error` | warning glyph + event-log line; errors++ |
| `harvest.truncated` | persistent glyph on device |
| `device.identity_conflict` | persistent glyph on device |
| `device.provisioning_needed` | dashed outline + "!!" |
| `scanner.degraded/recovered` | glyph on base box |
| `stream.gap` / WS drop | full-display stale treatment |

## Data-layer implications (step 17 backlog)

- Snapshot: resolve `jumper` from the owners registry directly (not only from
  harvest-time fold) so labels are immediate.
- New derived stat: **pending_download** = last `session_list.new_count`
  minus `store.session_added` since, per device (usually 0; nonzero during a
  harvest or after a failure — exactly when it matters). Additive snapshot
  field + statefold support.
- Per-device view-model: smoothed RSSI + tier (hysteresis), away timer,
  transfer state, badge count, warning flags.
- "Seen today" scoping for sky/stats (daemon-local midnight or config TZ —
  use the `[dropzone] timezone`).
