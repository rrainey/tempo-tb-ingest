# tempo-tb-ingest

Automated BLE harvesting of Tempo-BT skydiving log files, replacing manual SD-card
copying. An always-on dropzone workstation detects a returning device's BLE
advertisements (absent ≥ ~10 min ⇒ returned from a jump), connects over SMP, downloads
any sessions not already in the local store, and stages them for analysis by
`tempo-testbed` (`../tempo-testbed/device-data/<Device>/logs/<YYYYMMDD>/<SESSION>/flight.txt`).

Authoritative background: `docs/feasibility.md` (validated protocol facts, dropzone use
case, architecture decisions, risks, **validation history**). The other documents:
`docs/design.md` (architecture, event/snapshot contract, error policy — reconciled
against the implementation), `docs/implementation-plan.md` (step-by-step V&V plan with
per-step exit criteria and current status), `docs/dashboard-notes.md` (agreed dashboard
visual concept; step-18 implementation source), `docs/windows-options.md` (Windows
deployment options), `deploy/` (systemd unit, example config, install README). Dongle
firmware for the transfer-adapter pool (tuned Zephyr `hci_usb`) lives outside the repo
at `~/hci_usb` with its own build/DFU README.

## Deliverables (in implementation order)

1. **Ingestion daemon** — Python 3.12 asyncio; `smpclient`/bleak over BlueZ (Ubuntu 24).
   Components: continuous scanner → return detector → serialized harvest worker(s),
   local session index, and a structured real-time event stream (snapshot-first
   WebSocket + state-snapshot endpoint) designed in from the start. Plus a `promote` command
   (stage → `tempo-testbed/test-data/` analysis cases): formation grouping from log
   exit-times/GPS; jumper names and load organizer (= default formation base) from
   the user-maintained `device-owners.json` in the staging root, bound to sessions
   at harvest time; propose-and-confirm, never silent.
2. **Dashboard** — browser-based static SPA served by the daemon; full-screen,
   graphics-design-oriented near-real-time visualization of devices and ingest
   activity. Read-only v1; consumes only the event stream/snapshot API.

## Engineering approach: Verification & Validation

- **Automated testing wherever possible.** Unit tests for all pure logic (return
  detection, session diffing, path mapping, index, event schemas); integration tests
  against a **fake device/transport layer** so scanner→detector→harvest flows are
  testable without radios; BLE and filesystem access must sit behind narrow,
  mockable interfaces.
- **Test entry points**: `make check` = the offline gate (ruff + `mypy --strict` +
  pytest; hardware tiers excluded by default) — must be green before any step is
  called done. Hardware tiers are opt-in: `make live` (read-only, any Tempo-BT in
  range) and `make destructive` (dev device + `testok`-marked card only). Dashboard:
  `npm test` (vitest) in `dashboard/`.
- **Event-stream replay is a first-class test asset**: record real event streams
  (JSONL), replay them to drive the dashboard and regression tests without hardware.
- **Validation** = periodic end-to-end runs against a live Tempo-BT device (byte-level
  SHA-256 verification of downloads is the acceptance criterion). Record results in
  the style of `docs/feasibility.md` "Validation history".
- Fail loudly: no silent fallbacks or mock data on error paths (a specific
  tempo-insights lesson).

## Non-negotiable constraints

- **Production on-device logs are must-preserve.** Never delete or write to a device's
  storage unless the user explicitly directs it — with one exception: SD cards
  explicitly marked as test/scratch media by a marker **file** named `testok` at the
  filesystem root (`/SD:/testok`; may be empty or carry a card label). Destructive
  testing (writes, session-delete, wipes) is permitted on marked cards; absence of
  the marker = production. Probe over BLE with the stock fs STATUS command
  (`smpmgr file read-size /SD:/testok`): success = marked; `FILE_NOT_FOUND` =
  production (verified live 2026-07-08). Downloads/reads are always safe on any
  card. Automated tests that touch a real device must probe first and refuse
  destructive steps without the marker.
- **Device identity is the 4-char device-name suffix** (`Tempo-BT-0001` → id `0001`).
  The BLE MAC is randomly assigned at power-on — stable within a power-on session,
  never persistent across sessions; use it only as a transient connection handle.
  Devices advertising bare `Tempo-BT` (no suffix; legacy/unprovisioned) are rejected
  for processing until assigned a permanent suffixed name — surface them, never
  harvest them.
- Device paths: `/SD:/logs/<YYYYMMDD>/<8HEX>/flight.txt`. Session keys are
  `<YYYYMMDD>/<8HEX>` (session == jump), per firmware v1.5.0 `SESSION_LIST`.
  Do not copy protocol assumptions from `tempo-insights` — several are stale;
  `docs/feasibility.md` is the source of truth.
- Adapter roles (scan vs. transfer) are configuration: single-adapter mode (scanner
  paused per connection) and pool mode (dedicated scan adapter + up to four dongle
  transfer workers, never paused) are the same daemon (design §3.13). Configuration
  identifies adapters by **BlueZ controller address**, not `hciN` (indices depend on
  plug order; the dongles' HCI-level public address reads all-zeros, so resolution
  must go through BlueZ — `tempo-tb-ingest adapters` lists what's present).

## Related repos (same VS Code workspace)

- `../tempo-bt/zephyr/tempo-bt-v1` — device firmware (Zephyr/NCS, nRF5340); custom SMP
  group 64 in `src/mcumgr_custom.c`.
- `../tempo-insights/smpmgr-extensions/plugins/tempo_group.py` — smpmgr plugin for
  group 64; the manual/diagnostic harness (`smpmgr --ble <name> --plugin-path=... tempo ...`).
- `../tempo-testbed` — analysis app consuming the staged logs; `scripts/flight-info.sh`
  is the reference for extracting jump date/exit time from a log.
- `../tempo-core` — `@tempo/core` log parser (downstream consumer, not a dependency).
