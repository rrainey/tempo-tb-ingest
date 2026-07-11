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

### Implementation step 7 — 2026-07-08 (fault characterization, destructive tier)

Against dev device `Tempo-BT-0010` with a `/SD:/testok`-marked card (probe verified
True over BLE before any destructive step):

- **Failure surface characterized**: a mid-transfer link kill raises
  `smpclient.transport.SMPTransportDisconnected`; the sink retains a **byte-exact,
  chunk-aligned prefix** consistent with the last progress callback (295,936 of
  843,241 bytes in the characterization run). `smp_link` now maps this exception
  explicitly to `LinkDisconnected` (resumable).
- **Resume verified on hardware**: fresh connection + offset resume completed the
  file; SHA-256 identical to the uninterrupted baseline.
- Baseline throughput on this device/card: ~38.6 KB/s.
- Encoded as a permanent automated test: `tests/test_link_destructive.py`
  (`make destructive`), which hard-fails without the testok marker and performs
  kill-at-35% → prefix check → resume → byte-identity. Passed in 66 s.
- Operational note: a killed-but-undisconnected BleakClient corrupts subsequent
  D-Bus usage in-process (`OSError: Bad file descriptor` on reconnect) — always
  `disconnect()` a dead link before opening a new connection; the harvest worker's
  job cleanup does this by design.

### Implementation steps 8–9 — 2026-07-08 (scanner + presence, live bench)

Bench validation with shortened thresholds (`lost_after=30 s`, `absent_after=90 s`),
real radio, multiple devices staged by the operator:

| Check | Result |
|---|---|
| Away detection | ✅ `device.away` 31 s after Tempo-BT-0010 powered off (spec 30 s) |
| Return detection | ✅ `device.returned` with `absent_for_s=155.6` on reappearance + harvest trigger fired |
| Multi-device tracking | ✅ Tempo-BT-0001 and -0002 tracked concurrently (`device.new` + backlog triggers; 0002 later `device.away`) |
| Unprovisioned surfacing | ✅ bare `Tempo-BT` unit → `device.provisioning_needed`, never tracked for harvest |
| `device.seen` throttling | ✅ 1/s against a radio delivering several advertisements per second |

Note: 0010 resumed the same MAC across this power cycle — consistent with earlier
observations that addresses repeat in practice; identity remains name-suffix-keyed
regardless.

### Implementation step 12 — 2026-07-08 (live harvest validation)

Full pipeline (presence-triggered worker → smp_link → store/index) against real
devices, scratch staging root, live `device-owners.json` (0001→riley,
0007→scott_z LO). Devices 0001 (26 sessions incl. a real 0-byte `19700101` boot
artifact) and 0007 (2 sessions; renamed from bare `Tempo-BT` after its 1.5.0-reflash
settings wipe — second confirmed occurrence).

**Read-only pass:**

| Check | Result |
|---|---|
| `rebuild-index` on real staged data | ✅ 14 sessions indexed from a copied tree |
| Session diff | ✅ exactly the 12 unknown sessions selected, listing order preserved |
| Zero-byte session | ✅ refused loudly (`store.error`), harvest continued (worker fix made during this step: bad sessions skip, not abort) |
| Downloads | ✅ 13 sessions / 11.2 MB across both devices, ~44 KB/s effective |
| **Byte-identity acceptance** | ✅ deliberately-deleted `20260201/02E1741B` re-downloaded → SHA-256 identical to the manual SD copy |
| Harvest-time attribution | ✅ `riley` on 0001's sessions, `scott_z` (+LO flag) on 0007's |
| Cross-device evidence | ✅ scott_z `089D3D65` exit 15:22:58.125Z vs riley `00BAF6AB` exit 15:22:59.449Z — **1.3 s apart**: the real 20260705 2-way, ready-made ground truth for promote grouping |

**Destructive pass (0010, testok gate probed True first):** harvest killed at
~304 KB via link-kill → `harvest.failed(will_retry)` → sighting-driven retry →
`resumed_from=428032` → completed; SHA-256 identical to both the step-7 baseline
and an independent clean re-download.

A 1,269-event live recording from the read-only pass is preserved as
`tests/fixtures/live-harvest-20260708.jsonl` (dashboard development input).

### Implementation step 13 — 2026-07-08 (promote: grouping + proposals)

- **Parser port**: `flightinfo.py` matches `flight-info.sh` to the millisecond on all
  real-log truths, plus exit GPS positions. Real-data discovery: **V110 logs carry no
  `$GNRMC`** (no GNSS date) — exits are anchored via the session-key date fallback.
- **Golden grouping test**: the raw multi-device 20260206 logs (3 devices, 7 sessions)
  reproduce the three hand-built formations of test-data cases 02/03/04 exactly, with
  the default window (120 s) and GPS threshold (500 m) — settling design open
  question #5 empirically.
- **Live proposal** (scratch store from step 12, real registry): `--reattribute`
  bound 14 rebuild-indexed sessions live; the proposal produced
  `13-formation-20260705-2way` — riley `00BAF6AB` (exit 15:22:59.449Z) + scott_z
  `089D3D65` (exit 15:22:58.125Z, **base = load organizer**) — plus riley's 13:56
  solo, 11 single-device solos from earlier jump days, and 13 no-exit sessions
  correctly held for operator judgment. Confirmation prompt honored (`n` → nothing
  applied).

v1 gap noted: proposal application is all-or-nothing (no per-case selection yet);
design §3.11 allows editing/excluding — CLI selection flags deferred.

### Implementation steps 14–15 — 2026-07-08 (API + daemon assembly, live)

- **API (step 14)**: `/state`, `/healthz`, and a WS whose *first frame is a snapshot*
  taken after subscription — the snapshot/stream race is structurally impossible
  (design §3.7 amended). Wire format locked by a golden `/state` fixture; replay
  serves the API indistinguishably from live (verified against the real 1,269-event
  harvest recording: totals and attribution reproduced exactly).
- **Daemon (step 15)**: full assembly with single-instance flock, graceful shutdown
  (recorder drains after bus close; in-flight transfers leave resumable `.part`).
- **Design open question #3 answered on live hardware, in the negative**: BlueZ
  refuses connections while discovery runs (`org.bluez.Error.InProgress`, 5/5
  attempts failed). Fix: the harvest worker's radio gate now **pauses the scanner
  for each connection** (pause waits for the scan session to fully end; not an
  outage — no degraded events). With the gate in place, the live daemon: discovered
  0010 → paused → harvested its 843 KB session (SHA-256 identical to the step-12e
  reference) → resumed scanning; `failures: 0`.
- Live lifecycle checks: second instance refused with exit code 3 (lock held by a
  running daemon); SIGTERM → clean exit in **0.20 s** with `daemon.stopping`
  recorded and the lock released.

### Implementation step 16 (bench soak) — 2026-07-09 evening

Small-scale soak on the workstation: daemon + dashboard run manually
(`soak.toml`), real staging trees, operator's `device-owners.json`, five devices
with multi-weekend backlogs. ~25 min run, operator-observed via dashboard.

| Result | Detail |
|---|---|
| Devices | ✅ all 5 detected and harvested (0001, 0002, 0003, 0007, 0010) |
| Sessions | ✅ **22 sessions / 44.6 MB**, correctly organized under `device-data/` |
| Radio reliability | ✅ **zero transfer failures, zero retries** across 23 transfers (MTU 495 negotiated every connection) |
| Error paths | ✅ exactly one loud `store.error` — the known 0-byte `19700101/…` artifact, skipped by design |
| Baseline isolation | ✅ `rebuild-index --mark-baseline --except-date 20260705` kept promote scoped to new material |
| Shutdown | ✅ clean on SIGINT |

**Behavioral finding — away-flapping under paused scanning**: 15 `device.away` +
1 extra returned/harvest cycle among bench-bound devices. Cause: the scanner is
paused during connections (BlueZ requirement); a long backlog harvest (~10 min
for 0002) blinds sightings past `lost_after`, so idle devices age to AWAY (and
can re-trigger RETURNED) while sitting on the desk. Harmless here (the re-harvest
was a quick no-op) but visually wrong (devices "in the sky" on the dashboard) and
noisy. Candidate fixes: pause-aware presence aging (don't age while the scanner
is paused), and/or radio Option 2 (dedicated scan adapter). To be prioritized
with the operator's GitHub issues.

### Phase H step 21 (partial) — 2026-07-10 (hci_usb dongle throughput tuning)

Same payload (0010's 843 KB session), same host, byte-identity verified at every
step (SHA-256 = step-7 baseline):

| Dongle firmware config | Throughput |
|---|---|
| stock `hci_usb` (ACL 27 B × 3) | **19.0 KB/s** |
| + DLE 251 (`BT_CTLR_DATA_LENGTH_MAX`), ACL bufs 251 B, queues ×10 | **26.4 KB/s** |
| + SDC conn-event length 7.5 → 15 ms | 26.7 KB/s (no effect) |
| built-in Intel adapter (reference, step 7) | 38.6 KB/s |

Key learning: `CONFIG_BT_CTLR_DATA_LENGTH_MAX` (not the ACL buffer sizes) governs
the SDC's link-layer PDU length — buffer sizing alone changed nothing until DLE
was raised. The residual ~30 % gap to the built-in resisted the connection-event
lever; suspected fixed per-round-trip latency in the USB-FS HCI path (un-diagnosed:
a root btmon capture of the negotiated connection interval is the next probe if
pursued). Final dongle config: `~/hci_usb/prj.conf` (commented).

Pool-level perspective: 4 × 26.7 ≈ **107 KB/s aggregate**, ~2.8× the single
built-in adapter. Remaining single-link lever is device-side: fs-download chunk
1024 → ~2048 (fits the 2475 B netbuf), projected to lift the dongle path past
40 KB/s and the built-in past 60 — a fleet reflash, deferred to the next
tempo-bt firmware pass.

**Superseded same day by pipelining (below) — the serial protocol, not the
radio, was the ceiling.**

### Pipelined SMP downloads — 2026-07-10 (issue #2 follow-on)

Experiment: keep N fs-download requests in flight (SMP sequence numbers make
responses matchable). Same 843 KB payload via the dongle:

| Mode | Throughput | Integrity |
|---|---|---|
| serial production loop | 26.7 KB/s | ✅ |
| prototype window 2 | 75.9 KB/s | ✅ |
| prototype window 4 | 72.7 | ❌ lost chunk — device SMP netbuf pool overrun |
| **production `SmpLink`, window 2 (shipped)** | **58.4 KB/s** | ✅ byte-identical; resume-from-offset also 58.5 |

Implementation (permanent, on by default, window = 2): in-order sink writes
with out-of-order buffering; strict per-response verification (sequence, `off`
echo, chunk size); any anomaly → transport drain → automatic fallback to the
proven serial loop resuming from the contiguous prefix; real SMP errors and
disconnects raised, never masked. One upstream quirk fixed en route:
smpclient's BLE `receive()` cannot tolerate coalesced responses in its notify
buffer (raises; observed live) — `PipelinedSMPBLETransport` slices one message
and preserves the remainder. 10 offline tests over a scripted SMP transport
cover reordering, response loss (timeout→fallback), off-mismatch, error
frames, resume, and window discipline.

Consequences: custom dongle firmware / UART alternatives are moot; pool
projection 4 × ~58 ≈ **230 KB/s aggregate**; the built-in adapter also
benefits (unmeasured, expected ≥60 KB/s). The device-side chunk bump remains
available but is no longer pressing.

### Phase H steps 20–22 — 2026-07-10 (adapter pool foundations, live)

Fleet: built-in + **four flashed dongles** (5 controllers). Each new dongle
seized the system-default-adapter slot on attach — the address-based identity
design earns its keep continuously.

- **Step 20 ✅** — adapter identity/resolution (`adapters.py`, `tempo-tb-ingest
  adapters` CLI): all 5 controllers listed with hci↔address mapping (only
  obtainable via BlueZ D-Bus; dongles' HCI-level address is zeros); mode
  matrix unit-tested; default config resolves to single-adapter mode.
- **Step 21 ✅** — adapter-bound transport: `SmpLink(adapter="hciN")` binds
  discovery + connection to a named controller. Full live contract suite
  (11 behaviors incl. byte-identity + resume) passed **via dongle hci2 in
  46 s**. Two upstream quirks found and handled: the stock smpclient
  `_connect` ends with a `start_notify` that any override must reproduce
  (omitting it = no responses ever, cancellation chaos), and **bleak's
  deprecated `adapter=` kwarg mis-routes under parallel discovery** —
  100 %-reproducible `org.bluez.Error.InProgress` on N-1 of N simultaneous
  connects; the typed `BlueZScannerArgs` form routes correctly (3/3 parallel
  rounds clean). Both worth upstream reports.
- **Step 22 ✅** — cross-adapter concurrency proven live: **three concurrent
  downloads on three dongles (57.9 / 48.6 / 48.0 KB/s — per-link pipelined
  rates hold under concurrency)** while the built-in adapter scanned
  continuously — witness device saw 2,512 sightings with **max gap 0.3 s**
  across 107 s of triple transfer. BlueZ connect-during-other-adapter-
  discovery: works (the earlier `InProgress` finding was same-adapter only).
  Aggregate 8.6 MB in 108 s wall, bounded by one 6 MB file on a single link.

Remaining in Phase H: step 23 (worker pool, offline) and step 24 (full
1-scan + 4-transfer pool through the daemon, 👤).

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
