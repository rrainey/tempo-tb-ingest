# tempo-tb-ingest — Implementation Plan (V&V-oriented)

*Draft 2026-07-08. Executes `docs/design.md`. Each step has one objective, a defined
start point, and testable success. The automated test suite accumulates step by step;
everything added stays in the permanent regression body and must remain green for
every subsequent step.*

## Status — 2026-07-09

**Steps 1–15 complete**, each live-validated where marked 👤 (results in
`docs/feasibility.md` validation history). Offline gate: 260 tests. Notable
divergences discovered by real data/hardware and folded back into design + tests:

- **Step 12**: a real 0-byte session (`19700101/…` boot artifact) forced a worker
  change — bad sessions are skipped loudly, never abort the harvest (design §5
  updated).
- **Step 13**: V110-era logs carry no `$GNRMC` — exit UTC anchors on the
  session-key date as fallback. GPS grouping threshold (500 m) settled by the
  golden test (design open question #5 resolved).
- **Step 14**: WS protocol strengthened to a **snapshot-first frame** (design §3.7
  amended) — the snapshot/stream race is structurally impossible.
- **Step 15**: BlueZ rejects connections during discovery
  (`org.bluez.Error.InProgress`) — the worker's radio gate now pauses/resumes the
  scanner per connection (design §3.2 and open question #3 updated).

Remaining: step 16 (👤 field), step 17 (offline), step 18 (👤 gated on the
visual-design document; concept agreed in `docs/dashboard-notes.md`).

## Ground rules

- **A step is done when its exit criteria pass — never before, and no step starts
  until its predecessor's exit criteria are met.** Steps are sized so one step is one
  focused working session.
- **Verification** = automated tests (pytest; later vitest for the dashboard).
  **Validation** = observing the real system do the real thing (live device, user
  present where marked 👤). Every hardware validation result is appended to the
  validation history in `docs/feasibility.md`.
- The default test run (`make check`: ruff + mypy --strict + pytest) is entirely
  offline — no radios, no network, no device. Hardware tiers are opt-in markers:
  `-m live` (read-only, any Tempo-BT) and `-m destructive` (dev device, requires the
  `/SD:/testok` marker file — the probe is part of tier setup and hard-fails
  without it).
- No step may weaken a prior step's tests to pass (changing a test requires a stated
  reason in the commit).
- Destructive-anything on device storage: only under `-m destructive`, only after the
  `testok` probe (CLAUDE.md constraint).

## Step overview

| # | Phase | Step | Success is shown by | Status |
|---|-------|------|---------------------|--------|
| 1 | Foundation | Scaffold & toolchain | `make check` green on empty package | ✅ 07-08 |
| 2 | Foundation | Config module | unit tests | ✅ 07-08 |
| 3 | Foundation | Event model & bus | unit tests + golden schema fixtures | ✅ 07-08 |
| 4 | Foundation | Recorder & replay | round-trip tests + checked-in fixture | ✅ 07-08 |
| 5 | Protocol | Group-64 messages, `TempoDeviceLink`, `fake_link` | contract tests vs fake | ✅ 07-08 |
| 6 | Protocol | `smp_link` (real BLE) | contract tests vs live device 👤 (read-only) | ✅ 07-08 |
| 7 | Protocol | Fault characterization on hardware 👤 | fault catalog encoded in `fake_link` + resume test | ✅ 07-08 |
| 8 | Sensing | Scanner / `AdvertisementSource` | unit + live scan smoke 👤 | ✅ 07-08 |
| 9 | Sensing | Presence & return state machine | exhaustive unit tests (simulated clock); bench validation 👤 | ✅ 07-08 |
| 10 | Harvest | Store, session index & ownership registry | unit/integration on tmp trees | ✅ 07-08 |
| 11 | Harvest | Harvest worker end-to-end vs fakes | integration incl. fault scenarios | ✅ 07-08 |
| 12 | Harvest | Live harvest validation 👤 (read-only, then destructive) | byte-identity acceptance; resume on real interruption | ✅ 07-08 |
| 13 | Harvest | `promote`: grouping, attribution, proposals | golden proposals vs real multi-device logs; user review 👤 | ✅ 07-08 |
| 14 | Service | HTTP API: `/state`, `/events`, `/healthz` | WS contract tests, replay-driven | ✅ 07-08 |
| 15 | Service | Daemon assembly & CLI | full-loop integration vs fakes; clean-shutdown test; live daemon run 👤 | ✅ 07-08 |
| 16 | Service | Deployment & field soak 👤 | validation checklist at the dropzone | unit+watchdog ✅ 07-09; soak 👤 pending |
| 17 | Dashboard | App scaffold & data layer (+ daemon snapshot additions) | vitest on client logic; replay-driven demo | ✅ 07-09 |
| 18 | Dashboard | Visual implementation (per forthcoming design doc) | user review 👤 + kiosk soak | — |
| 19 | Portability | Windows laptop deployment (daemon + dashboard + testbed) | W0–W3 stages per `docs/windows-options.md` 👤 | — |
| 20 | Multi-radio | Adapter identity & resolution | unit matrix + live `adapters` listing 👤 | ✅ 07-10 |
| 21 | Multi-radio | Adapter-bound link + dongle throughput | live contract suite via dongle 👤; ≥50 KB/s pipelined | ✅ 07-10 |
| 22 | Multi-radio | Cross-adapter concurrency validation | scan uninterrupted during download 👤 | ✅ 07-10 |
| 23 | Multi-radio | Worker pool (offline) | overlap/serialization/chaos tests vs fakes | — |
| 24 | Multi-radio | Full-pool live validation (4 dongles) 👤 | N concurrent downloads; unplug chaos test | — |

👤 = requires user participation / hardware in range.

---

## Phase A — Foundation (no hardware anywhere)

### Step 1 — Scaffold & toolchain
- **Start point**: empty repo (this docs/ tree only).
- **Objective**: a testable, lintable, typed package skeleton and a one-command gate.
- **Work**: `pyproject.toml` (deps per design §8), `tempo_tb_ingest/` package with
  empty modules per design §3.1, `typer` CLI with `--version`/`--help`, `Makefile`
  (`check`, `test`, `live`, `destructive`), pytest/ruff/mypy configs, `tests/`
  layout with tier markers registered.
- **Verification**: `make check` runs ruff + `mypy --strict` + pytest and passes with
  a placeholder smoke test; `python -m tempo_tb_ingest --help` exits 0 (tested via
  subprocess in the suite).
- **Exit**: gate green; repo layout matches design §3.1.

### Step 2 — Config module
- **Start point**: Step 1 gate green.
- **Objective**: `config.py` — complete v1 surface from design §3.9.
- **Verification (tests added)**: defaults; TOML load; `TEMPO_INGEST_*` env
  precedence over file; validation errors on bad values (negative timeouts, empty
  adapter, unwritable paths flagged); round-trip of the documented example config
  verbatim from the design doc.
- **Exit**: unit tests cover every config field; gate green.

### Step 3 — Event model & bus
- **Start point**: Step 2 done.
- **Objective**: `events.py` — pydantic envelope (`v/seq/ts/type/data`), the full
  design §6.2 vocabulary as typed models, in-process bus with bounded per-subscriber
  queues, drop-oldest + `stream.gap`.
- **Verification (tests added)**: seq strictly monotonic under concurrent publishers;
  fan-out ordering per subscriber; slow consumer receives `stream.gap` with correct
  `dropped_count` and never blocks publishers; **golden JSON fixtures** for every
  event type (serialization locked — any schema change breaks a test deliberately).
- **Exit**: every event type in design §6.2 has a model + golden fixture; gate green.

### Step 4 — Recorder & replay
- **Start point**: Step 3 done.
- **Objective**: `recorder.py` — JSONL append with daily rotation; replay reader
  (`--speed`, `--loop`) publishing onto a bus.
- **Verification (tests added)**: record→replay round-trip reproduces envelopes
  byte-for-byte (modulo replay clock); rotation boundary; malformed-line handling is
  loud (skip + counted, never silent). A small **hand-written fixture recording**
  (synthetic harvest story) is checked in — it becomes the standing input for API
  and dashboard tests in Steps 14/17.
- **Exit**: `tempo-tb-ingest replay fixtures/synthetic-day.jsonl` runs; gate green.

## Phase B — Device protocol

### Step 5 — Protocol models, `TempoDeviceLink`, `fake_link`
- **Start point**: Step 4 done.
- **Objective**: `device/tempo_group.py` (group-64 messages ported from the smpmgr
  plugin), `device/protocol.py` (the link interface incl. the `testok` probe and
  `read_size`), `device/fake_link.py` (scripted device: fixture sessions on a tmp
  tree, configurable latency/throughput, fault-injection hooks).
- **Verification (tests added)**: message encode/decode round-trips (CBOR payloads
  match the schemas documented in feasibility/design — including `truncated`);
  **link contract test suite** written against the interface and executed against
  `fake_link` (the same suite runs against `smp_link` in Step 6 — one behavior spec,
  two implementations); fake-link fault hooks provably fire.
- **Exit**: contract suite green on fake; gate green.

### Step 6 — `smp_link` (real BLE) 👤
- **Start point**: Step 5 done; a Tempo-BT in range (production card fine — tier is
  read-only).
- **Objective**: `device/smp_link.py` on `smpclient`/bleak: connect by address,
  MTU exchange, `session_list`, `read_size`, `download(offset, sink)`, `testok`
  probe, disconnect.
- **Verification**: the Step-5 contract suite runs against the live device under
  `-m live`: session list parses and includes known sessions; `read_size` of a known
  file returns its known size; one full download SHA-256-matches the staged copy in
  `tempo-testbed/device-data/`; `testok` probe correctly reports *absent* on the
  production card.
- **Validation**: user observes the live run; result appended to feasibility history.
- **Exit**: `-m live` tier green with device present; offline gate untouched.

### Step 7 — Fault characterization 👤 (resolves design open question #2)
- **Start point**: Step 6 done; dev device with `testok`-marked card.
- **Objective**: learn `smpclient`'s real failure surface, then encode it.
  Scripted experiments: kill the link mid-download (power off / carry out of range /
  device starts logging), observe exceptions and partial-sink state; verify
  offset-resume completes and the result is byte-identical.
- **Verification (tests added)**: `fake_link` fault catalog updated to raise exactly
  what `smpclient` raises; offline integration test: interrupted download →
  `.part` retained → resume → SHA-256 identical. Repeatable henceforth without
  hardware.
- **Validation**: one real interrupted-and-resumed transfer, byte-verified
  (`-m destructive`, testok card).
- **Exit**: fault catalog documented in code; resume test in the permanent suite.

## Phase C — Sensing

### Step 8 — Scanner
- **Start point**: Step 7 done (no dependency on its hardware results; may start
  after Step 5 if hardware access is the bottleneck — the only hard prerequisite is
  the event bus).
- **Objective**: `scanner.py` — bleak active-scan wrapper emitting
  `AdvertisementSource` tuples; SMP-UUID/name filter; restart-with-backoff;
  `scanner.degraded/recovered` events.
- **Verification (tests added)**: filtering, tuple mapping, and backoff/recovery
  logic unit-tested with a fake bleak layer (scan-failure injection).
- **Validation 👤**: 60-second live smoke: real device appears in the source stream
  with name and RSSI (`-m live`).
- **Exit**: unit tests green; live smoke observed once.

### Step 9 — Presence & return detection
- **Start point**: Step 8 done.
- **Objective**: `presence.py` — the design §3.3/§3.4 machine: id-keyed states,
  unattributed (nameless/suffix-less) sightings held out, RSSI floor, `lost_after` /
  `absent_after` timers, post-harvest quiescence, `identity_conflict`.
- **Verification (tests added)**: exhaustive transition tests on a simulated clock —
  every documented transition, plus: flapping at the RSSI floor does not re-trigger;
  return before `absent_after` does not trigger; first-ever sighting triggers; bare
  `Tempo-BT` never reaches RETURNED but emits `provisioning_needed`; same id at two
  MACs → conflict + both blocked; device power-cycle mid-visit (new MAC, same id)
  keeps continuity. This is the logic core — target effectively full branch
  coverage.
- **Validation 👤**: bench run with `absent_after` shortened via config: power off /
  carry away the device, bring it back, observe `device.returned` in the event log.
- **Exit**: transition matrix tests green; bench validation observed.

## Phase D — Harvest

### Step 10 — Store, session index & ownership registry
- **Start point**: Step 9 done (only Phase A strictly required; ordered here to keep
  one-step-at-a-time).
- **Objective**: `store.py` — spool + atomic rename into the staging tree, SQLite
  schema (design §3.6 incl. `jumper`/`jumper_is_lo` attribution columns), diffing,
  dedup warning, `rebuild-index` — plus `owners.py`: the `device-owners.json`
  registry (design §3.12): parse/validate, suffix matching, mtime hot-reload,
  last-good-copy on error.
- **Verification (tests added)**: staging writes land at exactly
  `<root>/TempoBT-<id>/logs/<key>/flight.txt`; rename atomicity (no partial file
  visible under the final path, ever); diff correctness; duplicate-hash warn path;
  `rebuild-index` on a synthetic tree reproduces the DB (hashes recomputed);
  DB deleted → rebuilt → identical diff behavior. Registry: valid/invalid/duplicate
  entries, hot-reload on mtime change, `owners.error` keeps last good copy,
  unmapped lookup returns NULL attribution.
- **Exit**: gate green; a corrupted/absent DB is demonstrably recoverable; registry
  behavior fully covered.

### Step 11 — Harvest worker end-to-end (vs fakes)
- **Start point**: Steps 5, 9, 10 done.
- **Objective**: `harvest.py` — queue with coalescing, job state machine, radio
  lock, the design §3.5 pipeline, retry policy (re-queue on next sighting, max
  attempts, backoff).
- **Verification (tests added)**: full pipeline integration over `fake_link` +
  scripted advertisements: happy path (N new sessions stored + correct event
  sequence asserted against golden recording); truncated list; zero-byte file
  rejected; mid-transfer fault → retry on next sighting → resume → byte-identical;
  duplicate coalescing; max-attempts exhaustion is loud; `LOGGER_CONTROL` provably
  never sent (fake asserts on any group-64 write); harvest-time attribution recorded
  from the registry (incl. registry edited mid-run → next harvest uses new mapping;
  unmapped device → `jumper = NULL` + `owners.unmapped`).
- **Exit**: the synthetic "walk in the door" story runs offline end-to-end; its
  event log replaces/updates the Step-4 fixture recording.

### Step 12 — Live harvest validation 👤
- **Start point**: Step 11 done.
- **Objective**: the real thing, twice over.
  1. **Read-only** (`-m live`, any device): scratch staging root; daemon-lite run
     harvests any unknown sessions; every file SHA-256-verified against a manual
     SD copy. Acceptance = byte identity, per CLAUDE.md.
  2. **Destructive** (`-m destructive`, dev device + testok card): card imaged with
     known sessions; harvest; interrupt a transfer physically; observe resume;
     re-image card and verify re-harvest from empty index.
- **Validation**: user present; results appended to feasibility validation history.
- **Exit**: both runs recorded; any surprises fed back into fake-link faults or
  design (loop until clean).

### Step 13 — `promote`: grouping, attribution, proposals
- **Start point**: Step 10 done (Step 12 recommended first so real freshly-staged
  sessions exist to validate against).
- **Objective**: `promote.py` per design §3.11 — flight-info enrichment (port of
  `flight-info.sh`), formation grouping (exit-time window + GPS cross-check),
  case-proposal generation (`metadata.json` per `test-data/README.md`, dropzone
  from config, `baseJumper` = load organizer), propose-and-confirm apply, `--yes`,
  `--reattribute`.
- **Verification (tests added)**: flight-info parser vs real fixture logs (known
  dates/exit times from existing staged data); grouping unit tests — same-window
  formations, singleton solo, two groups on one load split by GPS, no-exit sessions
  excluded, missing/multiple LO flagged, unmapped sessions excluded; **golden
  proposal test**: the existing three-device formation logs in `device-data/`
  (20260206 / 20260228 — ground truth known from the hand-built `test-data` cases)
  + a fixture registry must reproduce the known-correct grouping and jumper
  assignments; apply idempotence (re-run creates nothing new); staging tree
  untouched by apply. The GPS threshold default is settled here (design open
  question #5) using those logs.
- **Validation 👤**: run against the real staged `20260705` sessions with your live
  `device-owners.json`; you review the proposal (and the generated `metadata.json`)
  before it applies.
- **Exit**: golden-proposal tests green; one real proposal reviewed and applied.

## Phase E — Service

### Step 14 — HTTP API
- **Start point**: Step 11 done (Steps 12/13 can proceed in parallel).
- **Objective**: `api.py` — `GET /state` (snapshot builder from live component
  state), `WS /events`, `GET /healthz`, static file serving; snapshot-then-stream
  semantics per design §6.
- **Verification (tests added)**: contract tests over aiohttp test client: snapshot
  `seq` coherence with subsequent WS events (no gap, no overlap) under concurrent
  publishing; reconnect → re-snapshot correctness; slow-WS-client gap marker;
  `/state` schema golden fixture; replaying the Step-11 fixture through the API
  yields the documented §6.1/§6.2 wire format exactly.
- **Exit**: contract suite green; wire format locked by fixtures.

### Step 15 — Daemon assembly & CLI
- **Start point**: Steps 12 & 14 done.
- **Objective**: `cli.py daemon` wiring scanner→presence→harvest→bus→API→recorder;
  single-instance lock; graceful shutdown (abort in-flight transfer cleanly, keep
  `.part`, emit `daemon.stopping`); structured logging.
- **Verification (tests added)**: whole-daemon integration on fakes: start → inject
  advertisement story → assert staged files + event log + `/state`; second instance
  refuses to start (lock); SIGTERM mid-transfer → `.part` retained, clean exit code,
  restart resumes.
- **Exit**: `tempo-tb-ingest daemon --config test.toml` survives the full synthetic
  story; gate green.

### Step 16 — Deployment & field soak 👤
- **Start point**: Steps 13 & 15 done.
- **Objective**: systemd unit (+ watchdog), install docs, and a real soak.
- **Verification**: unit file lints (`systemd-analyze verify`); watchdog feeds
  observed; journald logs structured.
- **Validation**: workstation at the dropzone (or a bench rehearsal first): a full
  jump-day cycle — devices leave, return, auto-harvest with correct jumper
  attribution from that day's `device-owners.json`, then `promote` groups the day's
  jumps and you confirm the proposal into `test-data/`. Tuning notes (absent_after,
  RSSI floor, exit_window) recorded; results appended to feasibility history.
  **This validates the central use-case requirement end-to-end.**
- **Exit**: soak report written; open tuning items filed into config defaults.

## Phase F — Dashboard

### Step 17 — Dashboard scaffold & data layer
- **Start point**: Step 14 done (replay + API are its complete dev environment; no
  hardware ever needed). Concept inputs: `docs/dashboard-notes.md` (2026-07-09).
- **Objective**: `dashboard/` Vite + React + TS scaffold; snapshot-first WS client
  (reconnect, re-snapshot on `daemon.started`, unmissable stale treatment); typed
  event models mirrored from the §6 contract; the **view-model layer the agreed
  concept needs**: per-device smoothed RSSI tier (EMA ~5 s + ~4 dBm hysteresis),
  away timers, transfer state (direction + rate for the bit animation), jumps-
  collected badge counts, warning flags, "seen today" scoping, event-ticker feed;
  a minimal unstyled state view proving the pipe (device roster + raw event feed).
  **Plus the daemon-side snapshot additions** from dashboard-notes: per-device and
  total `pending_download`, `conflicted` in the documented example, and `jumper`
  resolved from the owners registry on first sighting.
- **Verification (tests added)**: vitest on the client reducer: fixture replay in →
  expected view-model out (incl. tier hysteresis and away-timer derivation);
  reconnect logic; stale detection. Python side: statefold/daemon snapshot tests
  extended for `pending_download` and registry-resolved `jumper`; golden `/state`
  fixture regenerated deliberately. Build artifact served by the daemon
  (`GET /` integration test).
- **Exit**: `replay --loop` + browser shows live-updating roster; tests green.

### Step 18 — Dashboard visual implementation 👤
- **Start point**: Step 17 done **and** the user's dashboard visual-design document
  exists (concept and decisions already agreed in `docs/dashboard-notes.md`; the
  design doc owns look, motion, and typography).
- **Objective**: the full-screen, graphics-design-oriented visualization per that
  document, driven purely by the §6 contract.
- **Verification**: reducer/view-model tests extended for each new visual state;
  replay fixtures for demo scenarios checked in.
- **Validation**: user design review against replay-driven demo scenarios; kiosk
  soak (24 h, no leaks/stalls, stale indicator behaves on daemon restart).
- **Exit**: user sign-off; demo replay reproducible on any machine with a browser.

## Phase G — Portability

### Step 19 — Windows laptop deployment 👤
- **Start point**: Step 16 done (the Linux deployment is the reference
  behavior); options study `docs/windows-options.md` reviewed.
- **Objective**: run daemon + dashboard + tempo-testbed together on a Windows
  laptop. Primary approach: **Option A — daemon native on Windows** (bleak
  WinRT backend). Portability work: platform shim for the single-instance
  lock (`fcntl` → `msvcrt`/portalocker), Windows-safe signal handling,
  optional `[adapter] scan` (None = platform default), Windows config paths,
  service wrapper (NSSM or Task Scheduler), kiosk power-management checklist.
  Fallback (decision gate after W1): **Option B — WSL2 + usbipd-win** with the
  unchanged Linux stack. Docker is used, at most, for the testbed only.
- **Verification**: stage **W0** — the full offline gate passes on Windows.
- **Validation 👤**: stages **W1–W3** per the options study: live contract
  tier on the laptop (WinRT scan-response names, byte-identity), live daemon
  soak (noting whether WinRT also requires the scanner pause), and a full
  three-component jump-day rehearsal with a promote into `test-data\`.
  Results appended to the feasibility validation history.
- **Exit**: laptop runs all three components through a rehearsal day; chosen
  option and any WinRT findings recorded in `docs/windows-options.md`.

## Phase H — Multi-transceiver (radio Option 2; design §3.13, issue #2)

Hardware context: one flashed `hci_usb` dongle attached now; three more arriving.
Steps 20–22 need at most one dongle; step 23 is offline; step 24 needs the full
pool. Dongle firmware source + DFU package: `~/hci_usb`.

### Step 20 — Adapter identity & resolution
- **Start point**: Phase E complete; ≥1 dongle attached.
- **Objective**: config accepts BlueZ controller addresses or `hciN` for
  `adapter.scan`/`adapter.transfer`; startup resolution via BlueZ (D-Bus — the
  dongle's HCI-level public address is zeros, so `hciconfig` is useless);
  `tempo-tb-ingest adapters` utility lists controllers (address, hci name, bus);
  loud `ConfigError` for unresolvable/duplicated adapters; scan==sole-transfer ⇒
  single-adapter mode selected.
- **Exit criteria (tests)**: unit — spec parsing (address vs hciN), duplicate/
  missing adapter rejection, mode selection matrix; fake resolver injected.
  **Live check 👤**: `adapters` lists both the built-in and the dongle with the
  addresses `bluetoothctl list` shows.

### Step 21 — Adapter-bound link + dongle radio validation
- **Start point**: Step 20 done.
- **Objective**: adapter-bound SMP BLE transport (target discovery + connection on
  a named adapter) plumbed through `SmpLink(adapter=…)`; harvest worker passes its
  adapter. Measure dongle throughput; if far below the built-in's ~40 KB/s
  (suspected: 27-byte ACL buffers), retune `~/hci_usb/prj.conf` (DLE 251, ACL
  buffer count/size), rebuild, re-DFU, re-measure.
- **Exit criteria (tests)**: offline gate untouched (fake link has no adapter
  concept). **Live 👤**: the full link contract suite passes against a Tempo
  device *via the dongle* (`TEMPO_ADAPTER=<dongle-addr> make live`); byte-identity
  holds; measured dongle throughput recorded in feasibility history.
  *Throughput gate re-amended 2026-07-10: pipelined downloads (window 2,
  shipped as the SmpLink default with serial fallback) measured **58.4 KB/s via
  the dongle** in the production path — gate is now **≥ 50 KB/s per dongle
  link**, above the original 30. The earlier 25 KB/s relaxation is obsolete.*

### Step 22 — Cross-adapter concurrency validation
- **Start point**: Step 21 done.
- **Objective**: prove BlueZ allows connect-on-adapter-B during discovery-on-A
  (the `InProgress` failure was same-adapter). Scripted live run: continuous scan
  on built-in + full download via dongle.
- **Exit criteria**: **Live 👤** script asserts sightings continue throughout the
  download (max sighting gap ≪ `lost_after`, no `device.away` for bench devices);
  evidence appended to feasibility. If BlueZ refuses: fall back to
  scan-adapter-pauses-only-during-its-own-connects design note and re-plan.

### Step 23 — Worker pool (offline)
- **Start point**: Step 21 done (22 informs but doesn't block the code shape).
- **Objective**: `HarvestWorker` → pool: one worker task per transfer adapter over
  the shared coalesced queue; per-adapter serialization; single-adapter mode
  preserved bit-for-bit (pause gate + presence hooks); `adapter.lost/recovered`
  events; statefold/daemon snapshot `active_jobs` (additive; `active_job` = first);
  dashboard reducer/stats sum concurrent rates (scene already renders n beams).
- **Exit criteria (tests)**: integration over fakes — 4 workers × 6 devices:
  downloads provably overlap (fake pause hooks interleave), ≤1 job per device at a
  time, per-adapter serialization asserted from the fake call log; adapter-loss
  mid-job → job re-queued via sighting retry, other workers unaffected; full
  existing single-adapter suite passes unchanged; `/state` golden fixture
  regenerated deliberately with `active_jobs`; dashboard vitest extended for two
  simultaneous transfers.

### Step 24 — Full-pool live validation 👤
- **Start point**: Steps 22–23 done; all four dongles flashed & attached.
- **Objective**: the design §3.13 target configuration — scan on built-in, four
  transfer workers — against 4–5 real devices with fresh test sessions on marked
  cards where destructive steps apply.
- **Exit criteria**: **Live 👤** — N concurrent downloads observed (overlapping
  `transfer.progress` streams for ≥3 devices simultaneously); scanning
  uninterrupted (zero spurious `device.away`); aggregate throughput ≈ N× single;
  unplug-a-dongle chaos test: its job retries onto another adapter, pool shrinks
  loudly, daemon survives; results + tuning notes appended to feasibility
  validation history; dashboard shows multiple beams (screenshot for the record).

---

## The accumulated regression body

By step 18 the permanent, ordered test suite is:

1. toolchain smoke (S1) → config (S2) → event schemas incl. golden wire fixtures
   (S3) → record/replay round-trip (S4)
2. protocol codecs + link contract suite [fake & live-marker] (S5/S6) → fault
   catalog + resume (S7)
3. scanner logic (S8) → presence transition matrix (S9)
4. store/index/rebuild + ownership registry (S10) → harvest pipeline stories incl.
   faults and attribution (S11) → promote grouping + golden proposals vs real
   multi-device logs (S13)
5. API contract + wire-format fixtures (S14) → whole-daemon stories, lock, shutdown
   (S15)
6. dashboard reducer/view-model + served-artifact integration (S17/S18)
7. hardware tiers, run on demand: `-m live` (S6/S8/S12), `-m destructive`
   (S7/S12) — gated by the `testok` probe; promote validation on real staged
   sessions (S13 👤)

Fixtures (golden event JSON, synthetic JSONL recordings, fake-device session trees)
are versioned with the code; a contract change is visible as a fixture diff in
review, never as a silent drift.