# tempo-tb-ingest — Design Document (v1)

*Living document — drafted 2026-07-08, last reconciled against the implementation
2026-07-11. Companion to `docs/feasibility.md` (validated protocol
facts, use case, radio options, validation history). Where this document states a
protocol fact without citation, the feasibility study is the source.*

## 1. Purpose and scope

`tempo-tb-ingest` is an always-on ingestion service for a dropzone workstation. It
detects Tempo-BT devices returning from a jump via their BLE advertisements, harvests
new logging sessions over SMP, verifies and stages them for `tempo-testbed`, and
publishes everything it does as a structured real-time event stream. A browser-based
full-screen dashboard subscribes to that stream as a visual demonstration of the
system.

In scope for v1:

- The ingestion daemon (scanner, return detector, harvest pipeline, session index,
  event bus + HTTP/WS API, systemd deployment) — **implemented first**.
- The `promote` command (staging → `test-data/` analysis cases): semi-automated —
  formation grouping from log timestamps/GPS, jumper attribution from the
  user-maintained `device-owners.json` registry (§3.12), propose-and-confirm.
- The dashboard application shell and its data contract — **implemented second**;
  the visual/creative design was agreed in `docs/dashboard-notes.md` and implemented
  from it via iterative review (plan step 18); it consumes only the contract defined
  here in §6.

Out of scope for v1: firmware changes, device provisioning UI, deletion of sessions
from devices. **Multi-adapter pools**, originally v1-deferred, are now in scope as
the radio Option 2 implementation (§3.13; issue #2; plan Phase H).

## 2. System overview

```
                 advertisements                connections (SMP/BLE)
 Tempo-BT ──────────────┐                ┌──────────────────────────── Tempo-BT
 devices                ▼                ▼
              ┌───────────────────────────────────┐
              │ ingest daemon (Python 3.12 asyncio)│
              │                                   │
              │  Scanner ─► Presence/Return       │
              │             detector ─► Harvest   │
              │                         worker    │
              │      │          │          │      │
              │      ▼          ▼          ▼      │
              │      Event bus (in-process)       │
              │      │                     │      │
              │  WS /events + GET /state   │      │
              │  (aiohttp, also serves     ▼      │
              │   dashboard static files)  SQLite │
              └──────────┬────────────────────────┘
                         │                   │
                         ▼                   ▼
              Browser dashboard      staging tree:
              (kiosk, read-only)     tempo-testbed/device-data/...
```

One process, one asyncio loop, no threads except SQLite's short synchronous calls.
The daemon is the single owner of BLE interactions on its configured adapters
(adapter contention is real; see feasibility §risks).

## 3. Daemon architecture

### 3.1 Module layout

```
tempo_tb_ingest/
├── __init__.py
├── __main__.py            # python -m tempo_tb_ingest
├── cli.py                 # typer: daemon | promote | adapters | rebuild-index | replay
├── config.py              # TOML + env loading, validation, defaults (pydantic)
├── adapters.py            # BlueZ controller discovery + [adapter] role resolution (§3.13)
├── events.py              # event models, envelope, EventBus
├── scanner.py             # BleakScanner wrapper → advertisement stream
├── presence.py            # presence table + return-detection state machine
├── harvest.py             # job queue + harvest worker(s)
├── daemon.py              # composition root: wires components, radio gate, sd_notify
├── device/
│   ├── protocol.py        # TempoDeviceLink abstract interface
│   ├── smp_link.py        # real impl: smpclient over bleak; pipelined downloads (§3.5)
│   ├── tempo_group.py     # SMP group-64 messages (ported from smpmgr plugin)
│   └── fake_link.py       # scripted fake for tests (also used by replay/demo)
├── store.py               # staging-tree writer + SQLite session index
├── owners.py              # device-owners.json registry: hot reload, validation (§3.12)
├── statefold.py           # event stream → snapshot state (serves /state; resets on daemon.started)
├── api.py                 # aiohttp app: /state, /events (WS), static dashboard
├── recorder.py            # event stream → JSONL; replay JSONL → bus
├── flightinfo.py          # log parsing: jump date, exit UTC, GPS (flight-info.sh port)
└── promote.py             # interactive promote command
dashboard/                 # static SPA source (built → served by api.py)
tests/
```

Two seams make the system testable without radios (V&V requirement, CLAUDE.md):

- **`TempoDeviceLink`** — everything the harvest worker needs from a device:
  `connect() / session_list() / storage_info() / read_size(path) /
  download(path, sink, offset, progress) / probe_testok() / disconnect()`. `smp_link.py` is the real implementation; `fake_link.py` serves
  scripted sessions from local fixture files, with configurable latency, throughput,
  and fault injection (mid-transfer disconnects, truncated lists).
- **`AdvertisementSource`** — the scanner emits a plain async iterator of
  `(mac, name, rssi, uuids, timestamp)` tuples. Tests substitute a scripted source;
  the presence/return logic never imports bleak.

### 3.2 Scanner (`scanner.py`)

- `BleakScanner` with a detection callback on the configured scan adapter
  (`adapter.scan`, default `hci0`). A sighting qualifies if it carries the SMP
  service UUID (`8D53DC1D-…`) in the advertising data **or** a `Tempo-BT*` name in
  the scan response (either alone suffices: the name arrives in a separate scan
  response and the UUID is not always cached by BlueZ).
- Emits raw sightings onto the advertisement stream; no policy here.
- Restart-on-failure: if BlueZ discovery dies (D-Bus error, adapter reset), back off
  (1 s → 30 s cap), re-create the scanner, emit `scanner.degraded` /
  `scanner.recovered` events. The daemon never exits because scanning broke.
- Single-adapter mode (v1): the harvest worker holds a radio gate that **pauses the
  scanner for the duration of each connection** and resumes it after. This is
  mandatory, not hygiene: BlueZ rejects connect attempts outright while discovery is
  active (`org.bluez.Error.InProgress` — validated live 2026-07-08). Pause/resume is
  not an outage and emits no degraded/recovered events.

### 3.3 Device identity

The BLE MAC address is **randomly assigned at power-on** (firmware sets neither
`CONFIG_BT_PRIVACY` nor `CONFIG_BT_SETTINGS`): it is stable for the duration of a
power-on session (no RPA rotation) but must never be used as a persistent
identifier. (Observed addresses have repeated across reboots in practice; the design
must not rely on that.)

- **Canonical device identity = the four-character suffix of the device name**:
  `Tempo-BT-0001` → device id `0001`. Parse rule: `^Tempo-BT-(....)$`. Suffix
  uniqueness across the fleet is a provisioning discipline; the daemon detects
  violations (same id sighted at two addresses simultaneously) and emits
  `device.identity_conflict`.
- **MAC is a transient correlator**: it links sightings within a power-on session
  and is what bleak connects to. Harvest jobs target a device id and resolve it to
  the *most recently sighted* address at connect time.
- **Names arrive in the scan response**, so discovery must use **active scanning**
  (BlueZ/bleak default). A sighting is not attributable to a device id until its
  name is known; unnamed sightings are held un-attributed and do not drive state.
- **Legacy/unprovisioned devices advertising bare `Tempo-BT` (no suffix) are
  rejected for processing** — no presence tracking toward harvest, no downloads —
  until the device is assigned a permanent suffixed name. They are surfaced to the
  operator via `device.provisioning_needed` (and the dashboard) so the condition is
  visible, never silent.

### 3.4 Presence and return detection (`presence.py`)

Per-device state machine, keyed by **device id** (suffix):

```
                 seen                          absence ≥ absent_after (10 min)
   UNKNOWN ────────────► PRESENT ◄──────────┐      (evaluated lazily on next
      │                     │               │       sighting or periodic sweep)
      │ seen             not seen for       │
      │ (never seen      lost_after (90 s)  │ seen again within absent_after
      ▼  before)            ▼               │
   RETURNED ◄──────────── AWAY ─────────────┘
      │                     └── seen after ≥ absent_after ──► RETURNED
      ▼
   (harvest queued; on completion → PRESENT, quiescent until next AWAY cycle)
```

- `lost_after` (default 90 s): advertisement gap that moves PRESENT → AWAY. Long
  enough that normal advertising jitter and scan duty cycles don't flap it.
- `absent_after` (default 10 min): AWAY duration that makes the next sighting a
  RETURNED event. A first-ever sighting is also RETURNED (unknown backlog).
- **Hysteresis**: RSSI floor (default −88 dBm, config) applies to *sightings used for
  state transitions*; sub-floor sightings still update a `last_weak` field for
  dashboard display but don't change state. A device that completed a harvest is
  quiescent: further sightings keep it PRESENT; only a full AWAY→RETURNED cycle (or
  operator CLI `--force`) re-queues it.
- **Pause-aware aging** (single-adapter mode): the harvest radio gate reports
  scanner pause/resume to presence (`scanner_paused`/`scanner_resumed`), and
  paused intervals are excluded from every silence computation — a device
  unheard *because we stopped listening* is not absent. Without this, any
  harvest longer than `lost_after` marks every other device AWAY (observed in
  the first soak test). In pool mode (§3.13) the scanner never pauses and the
  hooks are never invoked.
- All transitions emit events (§6.2).

Rationale: the firmware is radio-silent while logging, so AWAY→RETURNED closely
tracks "jump completed" even if the jumper never physically leaves range.

### 3.5 Harvest pipeline (`harvest.py`)

A FIFO queue of harvest jobs (one per RETURNED device; duplicates coalesce). Worker
count = number of transfer adapters (one in single-adapter mode; up to four in pool
mode, §3.13). Job state machine — every transition evented:

```
QUEUED → CONNECTING → ENUMERATING → DOWNLOADING (per session file)
       → VERIFYING → STORING → DONE
any state → FAILED (categorized; retry policy below)
```

Per job:

1. **Connect** via `TempoDeviceLink` (holds the radio lock in single-adapter mode).
   Connect timeout 20 s.
2. **Enumerate**: `SESSION_LIST` (group 64) → full session keys
   `<YYYYMMDD>/<8HEX>`. If `truncated: true`, emit `harvest.truncated` (warning
   severity — the operator must eventually archive the card; feasibility risk #1).
3. **Diff** against the SQLite index → list of unknown session keys.
4. **Download** each unknown session's `/SD:/logs/<key>/flight.txt` (path by
   convention) to a `.part` file in a spool directory, emitting throttled
   `transfer.progress` events (≤ 5 Hz). SMP `fs download` is offset-based:
   a pre-existing `.part` resumes from its byte length. Downloads are
   **pipelined** (added 2026-07-10): `smp_link` keeps a small window of chunk
   requests in flight (default 2; ≥ 4 overruns the device's SMP netbuf pool),
   roughly doubling throughput (26.7 → 58 KB/s via a dongle). Any pipeline
   anomaly (lost response, offset mismatch) drains the transport of stale
   frames, then falls back to strictly serial chunking that resumes from the
   contiguous prefix already written — real device errors still raise; only
   transport anomalies degrade.
5. **Verify**: final size matches the `len` reported by the fs download protocol;
   compute SHA-256. Zero-byte or short files are failures, never stored.
6. **Store**: atomic rename into
   `<staging_root>/<DeviceFolder>/logs/<key>/flight.txt` (same-filesystem spool →
   rename is atomic). Record in index **with harvest-time jumper attribution**: the
   ownership registry (§3.12) is consulted at this moment — minutes after the jump,
   when the device→jumper mapping is freshest — and the binding (jumper name +
   load-organizer flag) is stored with the session. Emit `store.session_added`.
7. **Disconnect**, mark job DONE with summary stats.

**Failure policy.** Connection or mid-transfer failures (device left range, device
started logging and dropped BLE) leave `.part` files in place and schedule a retry:
the device's presence state returns to PRESENT-unharvested, and the *next sighting*
re-queues the job (no blind timer-based retries against an absent device); at most
`max_attempts` (default 5) per AWAY→RETURNED cycle, exponential backoff between
attempts while the device remains visible. All failures are loud, categorized events
— **no silent fallbacks, no mock data** (CLAUDE.md).

Never send `LOGGER_CONTROL` (or any group-64 write) during harvest: v1 harvesting is
strictly read-only against the device.

**Device folder naming.** Derived from the device id: `TempoBT-<id>` (existing
convention in `tempo-testbed/device-data/`, e.g. id `0001` → `TempoBT-0001`).
Suffix-less `Tempo-BT` devices are rejected for processing per §3.3 — they are never
queued, so no folder question arises.

### 3.6 Session index and staging store (`store.py`)

SQLite (stdlib, WAL mode) at `<data_dir>/ingest.db`:

```sql
CREATE TABLE devices (
  device_id TEXT PRIMARY KEY,    -- canonical identity: 4-char name suffix ("0001")
  name TEXT,                     -- last seen full BLE name
  folder TEXT,                   -- staging folder name ("TempoBT-0001")
  last_mac TEXT,                 -- most recent power-on-session address (transient)
  first_seen TEXT, last_seen TEXT,
  notes TEXT
);
CREATE TABLE sessions (
  device_id TEXT NOT NULL,
  session_key TEXT NOT NULL,     -- "<YYYYMMDD>/<8HEX>"
  size INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  downloaded_at TEXT NOT NULL,
  path TEXT NOT NULL,            -- staged file path
  jumper TEXT,                   -- harvest-time attribution (§3.12); NULL = unmapped
  jumper_is_lo INTEGER DEFAULT 0,-- jumper was load organizer at harvest time
  promoted_to TEXT,              -- test-data case/jumper, once promoted
  PRIMARY KEY (device_id, session_key)
);
CREATE INDEX sessions_sha ON sessions(sha256);  -- cross-device dedup check (warn-only)
```

The staging tree remains the human-browsable source of truth for file *content*; the
DB is the daemon's memory of what it has (diffing, dedup, promote bookkeeping). A
`rebuild-index` maintenance command reconstructs the DB by walking the staging tree
(hashes recomputed), so the DB is always disposable.

### 3.7 Event bus and API (`events.py`, `api.py`)

In-process pub/sub: components `publish(event)`; subscribers (WS sessions, recorder,
log writer) consume via bounded asyncio queues (slow consumers drop-oldest and
receive a `stream.gap` marker — the daemon never blocks on a viewer).

aiohttp application on `http.listen` (default `127.0.0.1:8080`; LAN exposure is a
config decision):

- `GET /state` — full snapshot (§6.1), including `seq` of the last event applied
  (polling/diagnostics).
- `GET /events` (WebSocket) — **the first frame is a snapshot**
  (`{"kind":"snapshot","state":{…}}`), taken after the server subscribes to the
  bus, followed by `{"kind":"event","event":{…}}` frames with `seq >` the
  snapshot's — so the snapshot/stream gap race is structurally impossible
  (amended 2026-07-08 from the original fetch-then-connect protocol, which was
  racy). On WS drop: reconnect; the fresh snapshot re-anchors (no server-side
  backfill in v1).
- `GET /healthz` — liveness for systemd watchdog / monitoring.
- `GET /` + static assets — the built dashboard (`dashboard/dist/`).

Read-only in v1: no control endpoints. (The bus design permits adding an
operator-command channel later — e.g. LED-blink identify — without rework.)

### 3.8 Recorder and replay (`recorder.py`)

- **Record**: every event (envelope included) appended to
  `<data_dir>/events/YYYYMMDD.jsonl`. Rotation daily; these files are the raw
  material for dashboard development, demos, regression fixtures, and field
  diagnosis.
- **Replay**: `tempo-tb-ingest replay <file.jsonl> [--speed 10] [--loop] [--listen
  HOST:PORT] [--static DIR]` starts the API server fed from the file instead of live
  components — the dashboard cannot tell the difference (`--listen --static` is the
  standard dashboard dev environment). This is a first-class deliverable, not a test
  hack: it is how the dashboard is developed and demoed without hardware.
- **Loop semantics**: each `--loop` cycle re-sequences events (offset past the
  previous cycle's max `seq`, so clients never see a repeated/decreasing `seq`) and
  injects a synthetic `daemon.started` at the cycle boundary, which resets
  accumulated state in every consumer (§6.2) — without this, folds double-count
  every cycle (bug found in the looping demo).

### 3.9 Configuration (`config.py`)

TOML at `/etc/tempo-tb-ingest.toml` or `--config`; env-var overrides
(`TEMPO_INGEST_*`) for containers/tests. Complete v1 surface:

```toml
[adapter]
scan = "hci0"            # scanning adapter
transfer = ["hci0"]      # transfer adapter pool (v1: same, single)

[detection]
rssi_floor_dbm = -88
lost_after_s = 90
absent_after_s = 600

[harvest]
connect_timeout_s = 20
max_attempts = 5
spool_dir = "/var/lib/tempo-tb-ingest/spool"

[store]
staging_root = "/home/riley/src/tempo-testbed/device-data"
data_dir = "/var/lib/tempo-tb-ingest"
# ownership registry is <staging_root>/device-owners.json (§3.12); override:
# owners_file = "..."

[promote]
test_data_root = "/home/riley/src/tempo-testbed/test-data"
exit_window_s = 120              # formation grouping window (§3.11)
gps_max_separation_m = 500      # freefall proximity cross-check

[dropzone]                       # copied verbatim into generated metadata.json
name = "Spaceland Dallas"
lat_deg = 33.4569
lon_deg = -96.3770
elevation_m = 233.0
timezone = "America/Chicago"

[http]
listen = "127.0.0.1:8080"
# static_dir = "dashboard/dist"  # built dashboard served at /; unset = API only

[log]
level = "info"           # structured JSON logs to stdout (journald)
```

(This example is pinned verbatim by `tests/test_config.py`.) `[adapter]` entries
accept either `hciN` names or BlueZ controller addresses; addresses are the stable
form for multi-adapter pools (§3.13), where an example appears.

### 3.10 Process management

systemd unit: `Restart=on-failure`, `WatchdogSec` fed from the main loop via
`sd_notify`, journald for logs, `After=bluetooth.service`. Single-instance enforcement
via a lock on `data_dir` (two daemons on one adapter is a known failure mode).
Graceful shutdown: finish or cleanly abort the in-flight transfer (leaving `.part`),
emit `daemon.stopping`, close WS sessions.

### 3.11 `promote` command (`promote.py`)

Semi-automated, **propose-and-confirm**: the command computes a complete promotion
proposal, displays it, and applies it only on operator confirmation (`--yes` skips
confirmation for scripting; individual proposals can be edited or excluded before
applying).

1. **Enrich** staged sessions where `promoted_to IS NULL`: jump date, exit UTC,
   duration, landing coordinates (Python port of
   `tempo-testbed/scripts/flight-info.sh` parsing: first `$GNRMC` date, `$GNGGA`
   extents, `$PST JUMPED` + `$PTH`/GGA clock alignment, sub-0.8 g fallback).
   Sessions with no detected exit are listed but not auto-grouped (candidate ground
   tests / non-jumps — operator decides).
2. **Group into formations**: sessions whose exit UTC fall within a configurable
   window (`exit_window_s`, default 120 s) form a candidate formation; a GPS
   cross-check (horizontal separation during freefall below a threshold) confirms
   membership and can split coincidental same-window groups. Singleton groups are
   solo jumps. Ambiguities (window overlap, GPS disagreement) are flagged in the
   proposal, never silently resolved.
3. **Build case proposals** per group, in the existing `test-data` conventions:
   - Case dir: next sequential number + generated slug —
     `NN-formation-YYYYMMDD-<k>way` / `NN-solo-<jumper>-YYYYMMDD`.
   - One subdir per jumper, named from the session's **harvest-time attribution**
     (§3.12); `flight.txt` copied in.
   - `metadata.json` generated per the `test-data/README.md` schema: `jumpers[]`;
     `baseJumper` = the group member whose `jumper_is_lo` is set (the load
     organizer is the default formation base) — if no LO is in the group, the
     proposal flags it and defaults to the first jumper pending operator edit; if
     more than one, flagged likewise; `isSolo`; `dropzone` block verbatim from the
     `[dropzone]` config section; `name`/`description` auto-composed with exit UTC
     and session hash IDs (matching the style of existing cases); `tags` seeded
     (`formation`/`solo`, `<k>way`, date).
   - Sessions with `jumper IS NULL` (unmapped at harvest) are excluded from
     grouping and listed prominently — fix `device-owners.json`, re-attribute
     (`promote --reattribute` re-reads the registry for still-unpromoted
     sessions), and re-run.
4. **Apply** on confirmation: copies only — staging remains intact; `promoted_to`
   recorded per session; re-running never duplicates an applied case.

### 3.12 Device ownership registry (`device-owners.json`)

The one piece of jump context that logs cannot provide: **who was wearing each
device, and who organized the load**. User-maintained (edited at the start of a jump
day or whenever a device changes hands), located at
`<staging_root>/device-owners.json` — alongside the data it describes:

```json
[
  { "deviceName": "Tempo-BT-0001", "jumperName": "riley", "isLoadOrganizer": true },
  { "deviceName": "Tempo-BT-0002", "jumperName": "russ" },
  { "deviceName": "Tempo-BT-0003", "jumperName": "divyatej_dt" }
]
```

- Fields: `deviceName` (full BLE name; matched by its 4-char suffix = device id),
  `jumperName` (must be a valid `test-data` jumper-directory name), and optional
  `isLoadOrganizer` (default `false`). The load organizer is the default formation
  base for analysis.
- **The daemon hot-reloads the file** (mtime check) before each harvest's store
  step; the binding is recorded per session at harvest time (§3.5) — attribution
  reflects who had the device *that day*, not whenever promotion happens to run.
- Same jumper on multiple devices is legal (e.g. test rigs). Duplicate
  `deviceName` entries, unparseable JSON, or invalid names are a **loud**
  `owners.error` event; the daemon keeps using the last good copy (never guesses,
  never blocks harvesting — files are stored with `jumper = NULL` only if the
  device has no valid entry).
- A harvested device with no registry entry is stored unattributed
  (`jumper = NULL`) and surfaced via `owners.unmapped` — visible on the dashboard,
  fixable later via `promote --reattribute`.

### 3.13 Multi-adapter operation — radio Option 2 (added 2026-07-10, issue #2)

Up to five BLE controllers used concurrently: **one dedicated scan adapter**
(continuous sniffing, never paused) and **up to four transfer adapters** forming a
query/download worker pool. Transfer hardware: nRF52840 Dongles running Zephyr's
stock `hci_usb` sample (source + DFU package retained in the workspace at
`~/hci_usb`), each appearing to BlueZ as an ordinary USB HCI controller.

**Adapter identity.** hci indices depend on plug order (the first dongle attached
even became the system *default* controller), so configuration identifies adapters
by **BlueZ controller address** (`bluetoothctl list`), with `hciN` accepted for
bench convenience. Caveat discovered on real hardware: the dongle's *public* BD
address reads all-zeros at the HCI level — its identity address (Zephyr static
random, FICR-derived, stable per chip across replug/reboot) exists only at the
BlueZ layer, so resolution goes through BlueZ (D-Bus), never `hciconfig`. A
`tempo-tb-ingest adapters` utility lists controllers (address, hci name, bus) to
make config authoring trivial.

```toml
[adapter]
scan = "44:A3:BB:E8:AD:D1"                  # built-in: continuous sniffing
transfer = ["DC:DF:ED:91:91:1D", "…", "…", "…"]   # dongle pool (1..4)
```

**Interaction model.**

```
   Scanner ──► Presence ──► shared FIFO job queue (coalesced: ≤1 job/device)
 (scan adapter,                    │
  never paused)        ┌──────────┼──────────┬──────────┐
                       ▼          ▼          ▼          ▼
                    worker A   worker B   worker C   worker D
                   (adapter 1)(adapter 2)(adapter 3)(adapter 4)
                       └───── each: one connection at a time ─────┘
```

- One worker task per transfer adapter, all pulling from the shared queue —
  N devices returning together harvest **N-at-once** (bounded by pool size).
  (Adapter-bound links and cross-adapter concurrency are implemented and
  live-validated; the worker-pool refactor itself is plan step 23.)
- Invariants: per-adapter serialization (a controller holds one connection at a
  time); per-device single job (existing queue coalescing); a failed job re-queues
  via the existing sighting-driven retry and may land on any adapter.
- **Mode selection is configuration**: if `scan` equals the sole `transfer` entry,
  the daemon runs exactly the validated single-adapter mode
  (`ScannerPausingRadioGate` + presence pause hooks). In pool mode the scanner is
  never paused and the pause hooks are simply never invoked — presence sees
  uninterrupted sightings (the issue-#2 symptom disappears at the root).
- **Adapter-bound links**: smpclient's BLE transport does not expose adapter
  selection; `smp_link` gains an adapter-bound transport subclass that performs the
  target discovery and connection on the worker's own adapter (a device must be
  discovered *by that controller* before BlueZ will connect it).
- **Failure containment**: a vanished adapter (unplug, USB reset) degrades only its
  worker — evented loudly (`adapter.lost` / `adapter.recovered`, additive
  vocabulary), pool capacity shrinks, scanning and other workers continue. The
  daemon never exits for adapter loss.
- **Contract**: snapshot gains `active_jobs: [0..n]` (additive; `active_job` is
  retained as its first element — §6.1 always said to treat it as 0..n). The
  `daemon.adapters` echo reports resolved roles.
- ~~Known hardware risk: stock `hci_usb` negotiates tiny host ACL buffers
  (27 B × 3)~~ — **resolved 2026-07-10**: the dongles run a tuned build
  (`CONFIG_BT_CTLR_DATA_LENGTH_MAX=251` is the decisive knob; ACL buffer sizes
  alone change nothing — build/flash instructions in `~/hci_usb/README.md`),
  lifting 19 → 26.7 KB/s; pipelined downloads (§3.5) then reach ~58 KB/s per
  dongle — *above* the built-in adapter's serial ~40 KB/s. The plan's
  throughput gate (step 21) is ≥ 50 KB/s, met with margin.
- ~~To validate live~~ — **validated 2026-07-10**: BlueZ permits connections on
  adapter B while adapter A discovers (witness scan: max sighting gap 0.3 s during
  three concurrent dongle downloads). Caveat encoded in `smp_link`: bleak's
  *deprecated* `adapter=` kwarg mis-routes under parallel discovery (instant
  `InProgress` on N−1 of N connects); the typed `BlueZScannerArgs` form is
  required.

## 4. Verification & Validation plan

Test tiers (pytest + pytest-asyncio; `ruff` + `mypy --strict` as static gates):

| Tier | Marker | Needs | Verifies |
|---|---|---|---|
| Unit | (default) | nothing | presence state machine (time-warped clock), session diffing, folder mapping, event schemas, config parsing, index ops, flight-info parsing against fixture logs |
| Integration | (default) | nothing | scanner→detector→harvest→store end-to-end over `fake_link` + scripted advertisements, incl. fault injection: mid-transfer drop + resume, truncated session list, zero-byte file, duplicate hash, WS snapshot/stream coherence |
| Replay | (default) | recorded JSONL fixtures | event-stream compatibility (a schema change that breaks stored recordings fails CI), dashboard data contract |
| Dashboard | (vitest) | nothing | view-model reducer (tier EMA/hysteresis, badges, reset-on-`daemon.started`), WS client reconnect/stale logic, golden wire fixtures shared with the Python side, real-recording replay |
| Live read-only | `-m live` | any Tempo-BT in range | discovery, session-list, one real download, SHA-256 match against a reference copy |
| Live destructive | `-m destructive` | dev device + **`testok`-marked SD card** | interrupted-transfer resume against a real radio drop, re-harvest after card reimage, (future) session-delete paths |

**`testok` protocol** (CLAUDE.md constraint): destructive-tier setup probes the card
for the root marker **file** `/SD:/testok` over SMP before anything else and
hard-fails the tier if absent. Probe = SMP fs `STATUS` (`smpmgr file read-size`;
`ReadFileSize` in smpclient) — verified live 2026-07-08 against firmware v1.5.0:
existing file → success + size; directory → `FS_MGMT_ERR_FILE_IS_DIRECTORY` (4);
missing → `FS_MGMT_ERR_FILE_NOT_FOUND` (3). A file marker is used because presence
maps to plain success (a directory would also be detectable via the distinct rc=4,
but interpreting error codes as "present" is needlessly subtle). The file may be
empty or carry a one-line card label. Local SD-card mounts check trivially either
way.

**Validation** (distinct from verification): before each field deployment, a scripted
end-to-end run against a live device — walk-away/return cycle, auto-harvest, SHA-256
byte-verification against a manual SD copy — with results appended to the validation
history in `docs/feasibility.md` style. Acceptance criterion is byte-identity, always.

Coverage isn't worshipped, but the presence state machine, harvest job state machine,
and event schema code must be effectively fully covered — they are the system's logic
core and are all testable without hardware.

## 5. Error handling summary

| Condition | Behavior | Event |
|---|---|---|
| Adapter/BlueZ failure | backoff + rebuild scanner; daemon stays up | `scanner.degraded/recovered` |
| Connect timeout | retry policy §3.5 | `harvest.failed(reason=connect)` |
| Mid-transfer disconnect | keep `.part`; resume on next attempt at byte offset | `transfer.failed(resumable=true)` |
| Device starts logging mid-harvest | same as disconnect (device drops BLE) | same |
| `truncated: true` in session list | harvest what's listed; persistent warning state until cleared | `harvest.truncated` |
| Short/zero-byte download | never stored; that session skipped loudly, the rest of the harvest proceeds (real case: a 0-byte `19700101/...` GPS-less boot artifact observed on 0001's card) | `store.error` |
| Duplicate SHA-256, different session key | store anyway; warn (possible card clone) | `store.duplicate_hash` |
| Unprovisioned name `Tempo-BT` (no suffix) | rejected for processing until renamed; surfaced for operator | `device.provisioning_needed` |
| Same device id at two addresses simultaneously | both blocked from harvest until resolved; loud warning | `device.identity_conflict` |
| `device-owners.json` invalid/unparseable | keep last good copy; harvest continues | `owners.error` |
| Harvested device absent from registry | stored with `jumper = NULL`; fixable via `promote --reattribute` | `owners.unmapped` |
| Formation grouping ambiguity (window overlap / GPS disagreement) | flagged in promote proposal; operator resolves | *(promote output, not an event)* |
| Staging disk full / unwritable | job FAILED loudly; daemon keeps scanning | `store.error` |
| Transfer adapter vanishes, pool mode (unplug/USB reset) | only that worker degrades; pool capacity shrinks; in-flight job re-queues via sighting retry; daemon stays up (§3.13; step 23) | `adapter.lost` / `adapter.recovered` |

No error path substitutes fabricated data or silently succeeds.

## 6. Daemon ↔ dashboard contract

This is the interface the dashboard (and its forthcoming visual-design document)
builds against. JSON throughout; all timestamps ISO-8601 UTC; `v: 1` schema version
in both snapshot and envelope. Additive changes only within v1; removals/renames bump
`v`.

### 6.1 Snapshot — `GET /state`

```json
{
  "v": 1, "seq": 8231, "ts": "2026-07-08T17:03:22.114Z",
  "daemon": { "version": "0.1.0", "started_at": "…", "adapters": {"scan": "hci0", "transfer": ["hci0"]},
              "scanning": true, "warnings": ["truncated:DC:BD:F1:0D:F1:D9"] },
  "devices": [{
    "id": "0001", "name": "Tempo-BT-0001", "folder": "TempoBT-0001",
    "mac": "DC:BD:F1:0D:F1:D9", "jumper": "riley", "is_lo": true,
    "state": "PRESENT", "rssi": -58, "last_seen": "…", "away_since": null,
    "sessions_known": 26, "pending_download": 0,
    "provisioning_needed": false, "conflicted": false, "truncated": false
  }],
  "queue": [ { "id": "…", "queued_at": "…" } ],
  "active_job": {
    "id": "…", "state": "DOWNLOADING",
    "session_key": "20260708/1A2B3C4D", "file_index": 2, "file_total": 3,
    "bytes_done": 1310720, "bytes_total": 2875691, "rate_bps": 43000
  },
  "totals": { "sessions_stored": 29, "bytes_stored": 88342511,
              "pending_download": 0, "harvests_completed": 7, "failures": 1 }
}
```

Dashboard-driven additions (2026-07-09, additive; implemented in the step-17
data layer — see `docs/dashboard-notes.md`):

- `conflicted` per device (identity-conflict glyph).
- `pending_download` per device and in `totals`: sessions discovered on the
  device (last `session_list.new_count`) minus commits since — usually 0,
  nonzero during an active harvest or after a failed one.
- `jumper` is resolved from the ownership registry directly (immediate on first
  sighting), not only from harvest-time attribution.

`active_job` is `null` when idle. Pool mode (§3.13; step 23) adds an additive
`active_jobs: [0..n]` list, with `active_job` retained as its first element — the
dashboard has always been required to treat active jobs as `0..n`.

`id` is the canonical device key throughout (§3.3); `mac` is informational — the
current power-on-session address — and may change between appearances of the same
`id`. Unprovisioned devices (bare `Tempo-BT`) appear in `devices` with `"id": null`,
`provisioning_needed: true`, and their transient `mac`, so the dashboard can show
them without the daemon ever processing them.

### 6.2 Event envelope and vocabulary — `WS /events`

```json
{ "v": 1, "seq": 8232, "ts": "2026-07-08T17:03:22.514Z",
  "type": "transfer.progress", "data": { … } }
```

`seq` is a per-daemon-run monotonic counter (resets on restart). `daemon.started`
is the universal reset signal: every stateful consumer (the daemon's own
`statefold`, the dashboard reducer) discards accumulated state when it arrives —
this is what makes loop-replay (§3.8) coherent — and a live client should also
re-snapshot, since events were missed while the daemon was down.

| Type | Key `data` fields | Notes |
|---|---|---|
| `daemon.started` / `daemon.stopping` | version, config echo | clients re-snapshot on `started` |
| `scanner.degraded` / `scanner.recovered` | reason | |
| `device.seen` | id, mac, name, rssi | throttled ≤ 1/s per device |
| `device.new` | id, mac, name, rssi | first sighting ever |
| `device.away` | id, away_since | PRESENT→AWAY |
| `device.returned` | id, absent_for_s | triggers queue |
| `device.lost` | id | pruned after prolonged absence (display only) |
| `device.provisioning_needed` | mac, name | bare `Tempo-BT` (no id); rejected for processing |
| `device.identity_conflict` | id, macs | same id at two addresses simultaneously (duplicate suffix in fleet) |
| `harvest.queued` / `harvest.started` | id, attempt | |
| `harvest.session_list` | id, count, new_count, truncated | |
| `harvest.truncated` | id | sticky warning |
| `transfer.started` | id, session_key, file_index, file_total, resumed_from | |
| `transfer.progress` | id, session_key, bytes_done, bytes_total, rate_bps | throttled ≤ 5 Hz |
| `transfer.completed` | id, session_key, bytes, sha256, duration_s | |
| `transfer.failed` | id, session_key, reason, resumable | |
| `store.session_added` | id, session_key, path, size, sha256, jumper | jumper `null` if unmapped |
| `store.duplicate_hash` / `store.error` | details | |
| `owners.reloaded` | entries, path | registry hot-reload (§3.12) |
| `owners.error` | reason, path | invalid registry; last good copy in use |
| `owners.unmapped` | id, name | harvested device with no registry entry |
| `harvest.completed` | id, sessions_downloaded, bytes, duration_s | |
| `harvest.failed` | id, reason, attempt, will_retry | |
| `stream.gap` | dropped_count | slow-consumer marker; client should re-snapshot |
| `adapter.lost` / `adapter.recovered` | adapter (address, hci) | pool mode only (§3.13); additive, lands with step 23 |

This vocabulary is intentionally rich enough to animate the full walk-in-the-door
story: appearance → return detection → connection → per-file progress → verified
storage.

## 7. Dashboard

- **Form**: static SPA (Vite + React + TypeScript, strict; bespoke SVG scene —
  no charting library), built to `dashboard/dist/`, served by the daemon; zero
  runtime dependencies beyond the daemon. Runs full-screen in Chromium `--kiosk` on the workstation; any other
  browser on the LAN may view simultaneously (multi-viewer is free).
- **Behavior**: snapshot-then-stream client per §6; read-only; auto-reconnect with
  re-snapshot; visible "stale" indicator if the stream drops (never silently
  frozen).
- **Dev/demo mode**: runs identically against `replay` (§3.8) — the visual design
  work needs no hardware present.
- **Concept** (agreed 2026-07-09; full brainstorm in `docs/dashboard-notes.md`;
  implemented at step 18 directly from those notes via iterative review, in place
  of a separate visual-design document): a dark, monochrome-green diorama of the
  dropzone. AWAY devices float above an "in the sky" line with away timers;
  visible devices sit in three RSSI tiers (EMA-smoothed with hysteresis, strongest
  at the bottom) as rounded-rect cards showing device index + jumper name and a
  jumps-collected badge; active transfers animate as an accent-colored bit stream
  (speed ∝ `rate_bps`) between device and base box; stats panel lower-right
  ("pending download" = discovered-not-yet-downloaded), event/history log, warning
  glyphs with a Help/Legend popup, unprovisioned devices dashed + "!!". A kebab
  context menu (reboot / identify / rename) is future work requiring v2 control
  endpoints.

## 8. Dependencies

Runtime: `smpclient` (SMP + BLE via bleak), `bleak`, `aiohttp`, `typer`, `pydantic`
(event/config models), stdlib `sqlite3`; `adapters.py` additionally uses
`dbus_fast` (already a bleak dependency) directly for BlueZ controller
enumeration. Dev: `pytest`, `pytest-asyncio`, `ruff`, `mypy`. Python ≥ 3.12;
packaged with `pyproject.toml` (`uv` for dev environments, plain `pip` install
supported). Dashboard: React 18 is the only runtime dependency; Vite,
TypeScript (strict), and `vitest` as the toolchain. Group-64 message classes are ported from
`tempo-insights/smpmgr-extensions/plugins/tempo_group.py` into
`device/tempo_group.py` (single source going forward; the smpmgr plugin remains the
manual diagnostic harness).

## 9. Milestones

Each milestone has explicit verification before the next begins (V&V approach).

| # | Deliverable | Verified by |
|---|---|---|
| M0 | Package scaffold, config, event bus, structured logging, CI-able test run | unit tests green; `--help` works |
| M1 | `device/` protocol client: session-list, download with resume, group-64 port | integration vs `fake_link` incl. fault injection; **live read-only tier vs Tempo-BT-0001: SHA-256 match** |
| M2 | Scanner + presence/return detection | unit (time-warped) + scripted-advertisement integration; bench test with real device power-cycled/carried away |
| M3 | Harvest pipeline + store + index | end-to-end vs fake; **live destructive tier vs dev device + `testok` card** (interrupted resume) |
| M4 | API (`/state`, `/events`), recorder, replay | WS contract tests; recorded live session replays cleanly |
| M5 | systemd deployment; field trial at dropzone | validation run appended to feasibility doc; threshold tuning notes |
| M6 | Dashboard v1 per `docs/dashboard-notes.md` | replay-driven demo; kiosk soak test |

Status (2026-07-11): M0–M4 complete; M5 deployment artifacts done, dropzone field
trial pending; M6 implemented, kiosk soak pending. A follow-on phase (multi-adapter
pool, §3.13) is in progress. Per-step status lives in
`docs/implementation-plan.md`; validation results in `docs/feasibility.md`.

## 10. Open questions (carried into implementation)

1. ~~`testok` probe mechanics over SMP~~ — **resolved 2026-07-08** (§4): marker is a
   root *file* `/SD:/testok`, probed with the stock fs STATUS command; all three
   response classes verified on live firmware.
2. ~~Exact `fs download` failure semantics in `smpclient` on radio drop~~ —
   **resolved 2026-07-08** (step 7, destructive tier): a link drop raises
   `smpclient.transport.SMPTransportDisconnected`; the sink retains a byte-exact,
   chunk-aligned prefix; offset-resume completes byte-identically. Encoded in
   `smp_link`'s mapping and `fake_link`'s fault catalog.
3. ~~Whether BlueZ requires explicit scanner stop during connect~~ — **resolved
   2026-07-08, live**: it does (`org.bluez.Error.InProgress` on every connect
   attempt while discovery ran). The harvest worker's radio gate pauses the
   scanner for the duration of each connection (`ScannerPausingRadioGate`);
   pause/resume is not an outage and emits no degraded/recovered events.
4. Unprovisioned-device operator flow (currently: surface only) — revisit after
   field trial.
5. ~~Formation-grouping GPS cross-check metric~~ — **resolved 2026-07-08**
   (step 13): metric is horizontal separation of *exit positions* (the GGA current
   at each session's exit event); `gps_max_separation_m = 500` validated by the
   golden test reproducing the hand-built 20260206 formations, and by the real
   20260705 2-way (exits ~40 m apart vs. the same-day solo ~800 m away).
6. ~~Dashboard visual design~~ — **resolved 2026-07-10**: implemented at step 18
   directly from `docs/dashboard-notes.md` via iterative review (no separate
   visual-design document was needed); continued visual refinement and the kiosk
   soak remain open under step 18.
