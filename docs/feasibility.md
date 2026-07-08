# Tempo-BT log ingest — feasibility and design study

*Updated 2026-07-07 (originally 2026-07-06). Sources: code exploration of
`tempo-bt/zephyr/tempo-bt-v1`, `tempo-insights`, and `tempo-testbed`; live-hardware
validation against `Tempo-BT-0001`; design discussion of the dropzone use case.*

## Bottom line

**Proven end-to-end on live hardware.** A stock Ubuntu 24 host — BlueZ, a standard BLE
adapter, Python — can discover a Tempo-BT device, enumerate its logging sessions,
and download byte-perfect log files at ~42 KB/s. No custom dongle firmware, no BLE
pairing, no root privileges. The firmware gaps found during the study (session
enumeration, transfer throughput) were closed in Tempo-BT firmware **v1.5.0** and
validated against the device.

Historical note: the tempo-insights code does **not** use an nRF52840 dongle or custom
host firmware, contrary to recollection. Its worker
(`tempo-insights/src/workers/bluetooth-scanner.ts`) shells out to `bluetoothctl` and the
Python `smpmgr` CLI over host BlueZ. (An unused alternate client wrapping the Go
`mcumgr` CLI exists at `tempo-insights/server/src/TempoBTClient.ts`; nothing
dongle-related survives in that repo.) Several of its hardcoded assumptions are stale
against current firmware — see *Validation history*.

## Target use case: dropzone auto-harvest

An always-on workstation at the dropzone. Skydivers return from a jump carrying their
Tempo-BT; the system notices them **as they walk in the door** and harvests
automatically. Central requirement:

1. Continuously observe BLE advertisements.
2. When a Tempo-BT device appears that has been **absent ≥ ~10 minutes** (consistent
   with departing for a jump), treat it as "returned from a jump".
3. Connect, obtain the session list, download any sessions the application store does
   not already have, verify, and store them.

Two firmware behaviors reinforce the absence heuristic:

- The device **stops advertising entirely while logging** (`logger.c:332`) — it is
  radio-silent for the ride to altitude and the jump even if it never leaves radio
  range, so "absent then reappeared" closely tracks "jump completed".
- The SMP service UUID is in the main advertising packet, so the observer can filter on
  it directly (and catch unprovisioned units advertising the default `Tempo-BT` name).

A second, human-in-the-loop stage (*promote*) maps harvested sessions to
tempo-testbed analysis cases; see *tempo-testbed data requirements*.

## Device firmware capabilities (as of v1.5.0)

From `tempo-bt/zephyr/tempo-bt-v1` (nRF5340 / u-blox NORA-B106, NCS 3.1):

- **SMP over BLE**, advertising the standard SMP service UUID
  (`8D53DC1D-1DB7-4CD3-868B-8A52-7460AA84`) with the device name (`Tempo-BT-*`,
  runtime-settable) in the scan response (`src/ble_mcumgr.c:40-45`). No pairing/bonding —
  the SMP characteristic is open R/W (`CONFIG_MCUMGR_TRANSPORT_BT_PERM_RW=y`), so a host
  can connect and transfer immediately.
- **Custom SMP group 64** (`src/mcumgr_custom.c`):

  | ID | Command | Op | Notes |
  |----|---------|----|-------|
  | 0 | SESSION_LIST | read | full session keys; see below |
  | 1 | SESSION_INFO | — | reserved, unimplemented |
  | 2 | STORAGE_INFO | read | backend, free/total bytes |
  | 3 | LED_CONTROL | write | |
  | 4 | LOGGER_CONTROL | write | never send during a transfer |
  | 5 | SESSION_DELETE | write | takes a full session key |
  | 6 | SETTINGS_GET | read | incl. `mag_mode` since ~v1.4 |
  | 7 | SETTINGS_SET | write | incl. `mag_mode` since ~v1.4 |
  | 8 | GET_DATETIME | read | |
  | 9 | TEST_LOGGING | write | synchronized multi-device start |
  | 10/11 | MAG_CAL_GET/SET | read/write | |

- **`SESSION_LIST` (reworked in v1.5.0, semantics chosen for this project — no prior
  API consumers existed):** session == jump. Two-pass walk of `/logs/<date>/<session>`;
  each entry is a fully-qualified session key:

  ```json
  { "sessions": [ {"name": "20260201/02E1741B"}, ... ], "count": 26, "truncated": false }
  ```

  The host reconstructs `/SD:/logs/<key>/flight.txt` by convention. Entries are maps so
  future fields can be added without breaking consumers. Bounds: 32 date dirs / 64
  sessions per response (sized to the 2475-byte SMP netbuf); overflow sets
  `truncated: true` rather than erroring.
- **Stock SMP FS group** (group 8) for `fs download`, chunk size **1024 bytes** as of
  v1.5.0 (`CONFIG_MCUMGR_GRP_FS_DL_CHUNK_SIZE`, was 256 → measured 3.5× throughput
  gain). An FS access hook (`src/ble_mcumgr.c:144-183`) allows reads of everything
  except `/lfs/system/`.
- **Log path**: `/SD:/logs/<YYYYMMDD>/<8-hex-session>/flight.txt`
  (`src/services/logger.c:678-729`; FAT mount `/SD:`; littlefs `/lfs` only as the
  no-SD-card fallback).
- **BLE is torn down while logging** (`logger.c:332`; restored on stop) — connectable
  only in IDLE/ARMED/post-flight states.
- Connection behavior: firmware requests a fast 7.5–15 ms connection interval on
  connect; L2CAP TX MTU up to 498; the host initiates MTU exchange (bleak does).
  Supervision timeout 4 s.
- **Settings live in the Zephyr settings partition** (internal flash): BLE name, PPS,
  PCB variant, mag mode/cal. A full-erase reflash wipes them all to defaults (observed
  2026-07-07). Restore via `tempo settings-set ...` + power cycle.

Host-side protocol support: `smpmgr==0.13.2` (pipx) + the group-64 plugin
`tempo-insights/smpmgr-extensions/plugins/tempo_group.py` (updated 2026-07-07: new
SESSION_LIST schema, `--json` output on `session-list`, `mag_mode` in settings
schemas).

## tempo-testbed data requirements

Two distinct trees, so ingest naturally splits into two stages:

1. **Auto-harvest** (fully automatable): mirror devices into
   `tempo-testbed/device-data/<DeviceName>/logs/<YYYYMMDD>/<SESSION>/flight.txt`. This
   staging tree already exists, exactly matches the on-device layout, and **no testbed
   code reads it** — automation can own it entirely.
2. **Promote** (inherently human-in-the-loop): copy into
   `tempo-testbed/test-data/<case>/<jumper>/flight.txt` plus a hand-authored
   `metadata.json` (`src/lib/testbed/data-loader.ts:5,76`; schema in
   `test-data/README.md`). Mapping session→jumper/case is not derivable from filenames —
   but `tempo-testbed/scripts/flight-info.sh` already extracts jump date, duration, and
   exit UTC from a log (GNSS `$GNRMC`/`$GNGGA` + `$PST JUMPED` + `$PTH` clock
   alignment): exactly the metadata a promotion CLI/UI shows a human.

Log format: single interleaved extended-NMEA text file per session
(`$PVER/$PIMU/$PIM2/$PENV/$PTH/$PST/$PMAG` + `$GNxxx`), parsed downstream by
`@tempo/core` (`tempo-core/src/analysis/log-parser.ts`, `dropkick-reader.ts`).
Multi-minute jump logs run to a few MB (observed: 2.9–6.2 MB).

## Host software stack options

### Option A — Python daemon on `smpclient` (recommended; selected)

Build `tempo-tb-ingest` as a Python daemon/CLI using Intercreate's
[`smpclient`](https://github.com/intercreate/smpclient) library directly (the library
underneath `smpmgr`). BLE rides on bleak → BlueZ D-Bus.

- One persistent connection per device per harvest (vs. one connection per `smpmgr`
  subprocess invocation).
- The group-64 message classes in `tempo_group.py` can be lifted nearly verbatim.
- Proper programmatic error handling — avoiding tempo-insights' silent mock-data
  fallbacks, absent retries, and stdout regex-scraping.
- The same bleak stack provides the continuous scanner (see radio options below) in
  the same asyncio process.
- State tracking with a local JSON/SQLite index; content-hash dedup retained from the
  insights design.

### Option B — Wrap the `smpmgr` CLI (validation harness)

`bluetoothctl` scan + `smpmgr --ble <name> --plugin-path ... tempo session-list --json` /
`file download`. This is what phase 0/0.5 used to validate everything, and it remains
the quickest manual/diagnostic harness. Not the daemon foundation: subprocess parsing,
a fresh BLE connection per command, coarse error handling.

### Option C — TypeScript-native (noble / node-ble) — rejected

No maintained Node SMP client; hand-rolling SMP framing + CBOR isn't worth it when the
Python stack is mature and this is a standalone tool.

### Option D — Web Bluetooth from the testbed app — rejected for v1

Browser SMP client + upload API is workable in principle but means an SMP
implementation in browser JS and a manual chooser dialog per connection — the opposite
of walk-in-the-door automation. Possible someday for one-off manual pulls.

## Radio and presence-detection options

The "sniffing" requirement needs no special hardware: BlueZ supports long-running BLE
observation with per-advertisement D-Bus events, exposed in Python by
`bleak.BleakScanner` (MAC, name, RSSI, service UUIDs per advertisement). The
return-detection logic — a presence table plus the ≥10-minute absence rule — is
ordinary application state, independent of which radio option is chosen.

### Radio Option 1 — single adapter (v1 baseline)

The workstation's built-in adapter both scans continuously and makes harvest
connections. BlueZ pauses discovery during a connection, so the scanner is blind while
a download runs (~1–2 min per jump log at measured rates). At dropzone timescales
(loads cycle every ~20 min) this is acceptable, and it requires zero extra hardware.
All phase-0 validation ran this way.

### Radio Option 2 — dedicated scan/transfer dongles (the scaling path)

Add USB BLE adapters and pin roles: one adapter scans continuously; harvest
connections go out on others. bleak supports selecting the adapter for both scanner
and client. Attractions beyond fixing the scan blind spot:

- **Radio-stack independence from the host machine.** An nRF52840 dongle flashed with
  Zephyr's stock `hci_usb` sample presents as a standard USB HCI Bluetooth adapter.
  The radio firmware is then pinned, identical hardware can be replicated across
  workstations regardless of what BT chipset each machine ships with, and HCI is the
  OS-neutral boundary (the same dongle works with any OS Bluetooth stack that speaks
  USB HCI). This is the legitimate modern form of the old "custom dongle" idea — with
  an *unmodified* Zephyr sample rather than bespoke firmware to maintain.
- **Concurrency scaling.** Multiple dongles allow simultaneous full-rate downloads
  (one connection per adapter) while scanning continues uninterrupted — relevant when
  a whole load of jumpers walks in together. (A single adapter *can* multiplex
  several LE connections, but they share radio time and contend with scanning; a
  dongle-per-transfer gives isolation and full per-link throughput.)

The daemon should treat "which adapter" as configuration (a scan adapter + a pool of
transfer adapters, defaulting to one shared adapter) so v1 → Option 2 is a config
change, not a redesign.

### Rejected: custom observer firmware on a dongle

A bespoke observer/presence firmware reporting over USB CDC would reproduce what BlueZ
passive scanning already provides while adding a firmware artifact and a host-serial
protocol to maintain. Its genuine advantages (promiscuous per-channel capture, precise
advert timing, hostless operation) don't serve this use case. Likewise Nordic's nRF
Sniffer is a Wireshark capture tool — wrong shape for presence detection.

## Proposed daemon architecture

One Python asyncio daemon, three components:

1. **Scanner** — continuous `BleakScanner` on the scan adapter (active scanning —
   names arrive in the scan response), filtered on the SMP UUID / `Tempo-BT*` names,
   maintaining a presence table `{device_id → last_seen, name, mac, rssi}`. Optional
   RSSI floor (~-75 dBm, tunable) so detection means "walked in", not "drove past".
2. **Return detector** — fires when a device is seen now and `last_seen` is ≥ 10 min
   old (or unknown). Hysteresis so a device flapping at radio-range edge doesn't
   re-trigger; after a successful harvest the device is quiescent until its next
   ≥10-min absence.
3. **Harvest worker(s)** — a queue of harvest jobs; with a single adapter, exactly one
   worker (kindest to the radio), with an adapter pool, one worker per transfer
   adapter. Per job: connect via `smpclient` → `SESSION_LIST` → diff against the local
   index → `fs download /SD:/logs/<key>/flight.txt` for each unknown session →
   verify (size + SHA-256) → store into `device-data/<Device>/logs/<key>/` → update
   index → disconnect.

Identity (corrected 2026-07-08): the firmware assigns the BLE MAC **randomly at
power-on**, so it is not a persistent identifier — the canonical device identity is
the **four-character device-name suffix** (`Tempo-BT-0001` → `0001`); the MAC serves
only as a transient connection handle within a power-on session. A generic `Tempo-BT`
advertisement (no suffix) is rejected for processing and flagged needs-provisioning
until the device is assigned a permanent suffixed name. See `docs/design.md` §3.3.

Separate `promote` command (interactive, run ad hoc): list unpromoted sessions with
date/exit-time/duration (port of `flight-info.sh` logic), prompt for case/jumper, copy
into `test-data/` and scaffold `metadata.json`.

## Live operations dashboard (placeholder)

An objective beyond the daemon itself: a visually appealing, graphics-design-oriented
**full-screen dashboard** depicting discovered devices and the interactions between
them and the ingestion host in near real time — serving as a visual demonstration of
the ingestion operations and the supporting hardware (e.g., at the dropzone or in
presentations).

Detailed requirements and visual design will be elaborated in a separate document.
One architectural implication is worth capturing now: the daemon should expose its
internal events (device seen/lost, return detected, harvest queued, connect, per-file
transfer progress, verify/store outcomes, errors) as a **structured real-time event
stream** (e.g., WebSocket/SSE plus a state-snapshot endpoint) rather than only logs,
so any dashboard front end can subscribe without touching harvest logic. Designing
the event bus in from v1 is cheap; retrofitting one is not.

## Risks and open questions (current)

Resolved during the study: session enumeration (v1.5.0 SESSION_LIST), throughput
(chunk 1024 → ~42 KB/s), path conventions (`/SD:` + `flight.txt` verified), pairing
(none needed).

Remaining:

1. **Session-list truncation at 64 sessions.** Logs are must-preserve (no auto-delete),
   so a device will eventually exceed 64 sessions and the response sets
   `truncated: true`. The daemon must surface this; the eventual fix is response paging
   or a date-filter parameter (the reserved SESSION_INFO id is available), or manual
   SD-card archival/cleanup.
2. **Interrupted transfers.** The device auto-arms at boot and auto-starts logging on
   climb detection, which drops BLE mid-transfer. Treat partial downloads as
   resumable (SMP `fs download` is offset-based); never issue LOGGER_CONTROL during a
   transfer (a BLE-initiated logging start intentionally drops the radio after 500 ms).
3. **Settings wipe on full-erase reflash** (observed): name, PPS, PCB variant, and
   possibly mag calibration revert to defaults. Operational concern rather than a
   daemon concern, but the daemon's needs-provisioning flag is the detection point.
4. **Single-adapter scan blind spots during downloads** (v1) — accepted; Option 2 is
   the config-level fix.
5. **Threshold tuning.** The 10-minute absence rule, RSSI floor, and hysteresis all
   need field tuning at the dropzone.
6. **Unprovisioned-device policy.** Ignore, alert, or offer provisioning at the
   workstation? Affects whether the daemon needs any notification/UI channel in v1.

## Validation history

### Phase 0 — 2026-07-06 (firmware pre-1.5.0, stock smpmgr + plugin, built-in adapter)

| Question | Result |
|---|---|
| Discovery | ✅ `bluetoothctl` scan found `Tempo-BT-0001` (`DC:BD:F1:0D:F1:D9`) in <25 s |
| `tempo session-list` | ⚠️ worked, but returned only top-level date dirs (max 20, dirs only) — no session IDs; confirmed the firmware enumeration gap |
| Path convention | ✅ `/SD:` prefix + `flight.txt` confirmed via successful `file download`; tempo-insights' `/lfs/` + `flight.dat` paths confirmed stale |
| Integrity | ✅ SHA-256 of BLE download identical to the manual SD copy (134,148 bytes) |
| Throughput | ~12 KB/s at 256-byte chunks (134 KB in ~13.5 s incl. connect) |
| `tempo storage-info` | ✅ backend `sdcard`, 32 GB card |

### Phase 0.5 — 2026-07-07 (firmware with SESSION_LIST rework + 1024-byte chunks, now v1.5.0)

| Check | Result |
|---|---|
| `session-list --json` | ✅ 26 sessions, full `date/id` keys, `truncated: false` |
| New-session harvest | ✅ all three previously-unseen `20260705` jumps downloaded over BLE and staged into `tempo-testbed/device-data/TempoBT-0001/logs/20260705/` |
| Log validity | ✅ `flight-info.sh` parses all three (2 detected exits 13:56 / 15:22 UTC; third is a 42.6-min log, no exit) |
| Integrity regression | ✅ re-download of `20260201/02E1741B` at 1024-byte chunks SHA-256-identical to the manual SD copy |
| Throughput | **~42 KB/s** (2.9 MB in 70 s; 3.5× baseline). 134 KB file: 13.5 s → 3.7 s |

Also during phase 0.5:

- Full-erase reflash wiped the settings partition (name reverted to `Tempo-BT`);
  restored via `tempo settings-set --ble-name Tempo-BT-0001` + power cycle.
- Plugin schema drift found and fixed: firmware ≥ ~v1.4 added `mag_mode` to the
  settings responses, which crashed the plugin's pydantic validation; both settings
  response models now carry `mag_mode: Optional[int]`, tolerant of old and new
  firmware.

### Implementation step 6 — 2026-07-08 (smp_link live contract run, read-only)

The `TempoDeviceLink` contract suite (10 behaviors) ran against `Tempo-BT-0001` over
BLE via the new `smpclient`-based `smp_link` (`make live`, 43.9 s, all passed):

| Behavior | Result |
|---|---|
| SESSION_LIST shape (known sessions ⊆ keys, `truncated` bool) | ✅ |
| fs STATUS: known file exact size; missing → FILE_NOT_FOUND; directory → FILE_IS_DIRECTORY | ✅ |
| Full download of `20260201/02E1741B` | ✅ SHA-256 identical to staged reference |
| Offset-resume (head[:n] + tail-from-n) | ✅ SHA-256 identical |
| Progress callbacks monotonic and complete | ✅ |
| `testok` probe on production card | ✅ correctly absent |
| Read-only guarantee (no write-shaped calls in the call log) | ✅ |

Connect-retry (4 × 3 s) absorbed the known miss-discovery-after-disconnect behavior;
zero flakes across 10 sequential connect/disconnect cycles.

## Roadmap

1. **v1 daemon (Radio Option 1, Option A stack):** scanner + return detector + single
   harvest worker; local session index; MAC-keyed identity; `truncated` surfaced;
   systemd unit for always-on operation. Plus the interactive `promote` command.
2. **Field tuning:** absence threshold, RSSI floor, hysteresis; observe multi-device
   afternoons.
3. **Radio Option 2 when scale demands:** adapter-role configuration (scan adapter +
   transfer-adapter pool); candidate hardware includes nRF52840 dongles running
   Zephyr's stock `hci_usb` sample for pinned, replicable radio behavior.
4. **Live operations dashboard:** full-screen near-real-time visualization of devices
   and ingest activity, fed by the daemon's event stream (separate design document to
   follow; see placeholder section above).
5. **Later firmware niceties:** SESSION_LIST paging/date-filter (or SESSION_INFO),
   richer session entries (e.g. file size for progress reporting).
