# Running the ingest stack on a Windows laptop — options study

*2026-07-09. Goal: run the ingestion daemon, the ingestion dashboard, and the
tempo-testbed analysis app together on a single Windows laptop at the dropzone.
This document weighs the packaging approaches and their data-sharing and
testing consequences, and proposes an implementation step.*

## 1. The shape of the problem

Three components, very different portability profiles:

| Component | Needs | Windows portability |
|---|---|---|
| **Ingestion daemon** (Python) | **Bluetooth LE** (scan + connect), filesystem, SQLite, HTTP | The only hard problem. Everything except BLE is trivially portable. |
| **Dashboard** | a browser pointed at the daemon | Free — it's a static SPA served by the daemon; Edge/Chrome kiosk mode works anywhere. |
| **tempo-testbed** (Next.js) | Node.js, reads `test-data/` from its project root | Runs natively on Windows Node or in Docker; no hardware dependencies. |

So the decision is really: **where does the daemon's BLE stack live?** Three
candidate answers: Windows' native BLE stack (WinRT), a Linux stack inside
WSL2, or a container. Spoiler: the container answer is a dead end for BLE.

## 2. Option A — daemon native on Windows (recommended candidate)

bleak — the BLE library under both our scanner and `smpclient` — has a
first-class **WinRT backend**, and smpclient's own support matrix lists
Serial/BLE/UDP as ✅ on Windows 11. So the daemon can plausibly run as a plain
Windows Python process using the laptop's built-in Bluetooth. No VM, no
passthrough, no container.

**Required daemon adaptations** (small, enumerable — none touch core logic):

1. **Single-instance lock**: `daemon.py` uses `fcntl.flock` (Unix-only).
   Portable alternative: `msvcrt.locking` behind a tiny platform shim, or the
   `portalocker` package.
2. **Signal handling**: `loop.add_signal_handler` is not supported on the
   Windows Proactor event loop. Fallback: `signal.signal` +
   `loop.call_soon_threadsafe`, or rely on `KeyboardInterrupt`/service-stop.
3. **Adapter naming**: config default `hci0` is meaningless on WinRT (bleak
   ignores/errors on the `adapter` kwarg there). Make `[adapter] scan`
   optional (None = platform default); the scanner already passes the kwarg
   conditionally.
4. **Default paths**: `/var/lib/tempo-tb-ingest`, `/etc/…` become Windows
   paths via config (no code change — the config surface already covers it).
5. **Process management**: no systemd. Options: **NSSM** (wraps the daemon as
   a Windows service, restart-on-failure), Task Scheduler at-logon, or a
   plain startup shortcut for a kiosk laptop. The `/healthz` endpoint remains
   the liveness probe.
6. **WinRT behavioral unknowns to verify** (not assumed): active-scan name
   delivery (identity depends on scan-response names), detection-callback
   cadence for the RSSI tiers, connect-while-scanning behavior (Windows may
   not need the scanner pause that BlueZ requires — the radio gate is kept
   regardless; pausing is harmless), MTU negotiation (smpclient contains
   WinRT-specific MTU handling, a good sign).

**Verification is cheap because of the V&V suite**: the offline gate should
pass on Windows once the `fcntl` shim lands; then the existing `-m live`
contract tier against a real device answers the WinRT unknowns in under a
minute (`make live` equivalents; byte-identity acceptance unchanged).

**Data sharing (Option A)**: everything is ordinary Windows filesystem —

```
C:\tempo\
├── tempo-testbed\            (git checkout; `npm run dev` or a built server)
│   ├── device-data\          ← daemon staging_root (+ device-owners.json)
│   └── test-data\            ← promote target; testbed reads this
└── ingest-data\              ← daemon data_dir (index DB, spool, events)
```

Same layout as Linux, different root. `promote` runs on the same machine, so
the copy into `test-data\` is a local file operation. The dashboard is served
by the daemon on `localhost:8080`; the testbed runs on `localhost:3000`.

## 3. Option B — Linux stack in WSL2 + USB passthrough

Keep the daemon binary-identical to what we've validated: Ubuntu in WSL2 with
BlueZ, the adapter delivered by **usbipd-win** (attaches a USB device —
a BLE dongle, possibly the laptop's internal BT module — into WSL2).

- **The catch**: the stock WSL2 kernel does not ship `btusb`/Bluetooth
  modules; a custom-compiled WSL kernel (documented but nontrivial to
  maintain across WSL updates) is typically required, plus BlueZ installed in
  the distro and usbipd re-attach after every reboot/replug (scriptable).
- **What you gain**: zero daemon changes — the exact stack validated in steps
  6–15, including BlueZ-specific behaviors (the scanner-pause requirement is
  a BlueZ finding; it simply remains correct here).
- **Everything else fits naturally**: WSL2 supports systemd now (our unit
  file works); tempo-testbed runs in WSL too (Node on Linux); **keep all data
  inside the WSL filesystem** (`/home/...`) — crossing `/mnt/c` is an order
  of magnitude slower and this workload is file-heavy. Windows-side browsers
  reach WSL services via automatic localhost forwarding (dashboard :8080,
  testbed :3000), so the kiosk experience is identical.
- **Fragility budget**: usbipd attach state, WSL kernel updates, and BT-dongle
  enumeration are the moving parts. A startup script (attach → verify
  `hciconfig` → start daemon) mitigates; the daemon's scanner-degraded
  backoff already survives adapter disappearance loudly.

This is the right fallback if Option A's WinRT verification surfaces real
gaps (e.g., unreliable scan-response names), and the right choice if we ever
want the **nRF52840 `hci_usb` dongle** path (radio Option 2 from the
feasibility study) — that dongle would also attach via usbipd and behave as a
standard BlueZ adapter, keeping radio behavior identical across the Linux
workstation and the Windows laptop.

## 4. Option C — Docker: useful for the testbed, a dead end for the daemon

Two distinct verdicts:

- **Daemon in Docker on Windows: no.** Docker Desktop containers run inside
  Docker's own WSL2 VM whose kernel you don't control — no `btusb`, no BlueZ,
  no host-Bluetooth passthrough. Every "BLE in Docker" recipe ultimately
  requires a host Linux Bluetooth stack to bind to; on a Windows host there
  isn't one. (Even on Linux hosts, we'd need `--net=host` + D-Bus mounts, as
  tempo-insights did — worth it only when the host is Linux anyway.)
- **tempo-testbed in Docker: fine but optional.** A standard Node image with
  `test-data/` bind-mounted works anywhere Docker Desktop runs. But native
  Windows Node is one installer and avoids Docker Desktop's footprint on a
  field laptop; Docker adds value mainly if we want a one-command
  `docker compose up` for the analysis app and are already paying the Docker
  Desktop cost.
- A future **Linux mini-PC deployment** (Intel N100 class) is where Docker
  packaging of the daemon becomes attractive (host BlueZ + `--net=host`, as
  proven by tempo-insights' packaging) — worth keeping in mind as the
  "appliance" endgame, distinct from the Windows-laptop scenario.

## 5. Cross-cutting operational notes (any option)

- **Power management is the #1 field risk on a laptop**: Windows will
  suspend USB/BT radios and sleep the machine. Kiosk configuration must
  disable sleep, USB selective suspend, and BT power saving; on WSL, add
  usbipd re-attach on resume.
- **Firewall**: allow inbound :8080 (dashboard from other devices on the LAN)
  and :3000 (testbed) if multi-viewer is wanted; localhost-only needs nothing.
- **Time**: exit timestamps come from GPS in the logs, so laptop clock skew
  doesn't corrupt data — but "seen today" scoping and event timestamps use
  the local clock; normal NTP is sufficient.
- **Backups**: the staging tree is the source of truth; a scheduled robocopy/
  rsync of `device-data\` covers it (the index DB is disposable by design).

## 6. Testing & validation plan (proposed Step 19)

Reusing the existing tiers — the suite is the portability instrument:

| Stage | What | Acceptance |
|---|---|---|
| W0 | Offline gate on Windows (`make check` equivalent; `fcntl` shim + path fixes land here) | 260+ tests green on Windows CI or the laptop |
| W1 | Live read-only tier on the laptop (WinRT verification for Option A) | contract suite green vs a real device; scan-response names present; byte-identity |
| W2 | Live daemon soak on the laptop: discovery → pause/connect → harvest → dashboard | same acceptance as step 15's live run; note whether WinRT needs the scanner pause |
| W3 | Full stack rehearsal: daemon + dashboard kiosk + testbed, promote a session end-to-end | jump-day dry run identical to step 16's checklist |

Decision gate after W1: if WinRT verification fails materially → fall back to
Option B (WSL2 + usbipd), rerunning W1–W3 there.

## 7. Recommendation

**Try Option A first** (native Windows daemon): smallest moving-parts budget,
one Python install, and the existing test tiers can confirm or refute it in
an afternoon with a device on the desk. **Hold Option B in reserve** — it is
guaranteed to work (it *is* the validated stack) at the cost of usbipd/kernel
ceremony. **Use Docker only for the testbed, and only if convenient**; never
for the daemon on Windows. Either way, all three components share data as
plain files on one filesystem, with `device-data\` under the testbed checkout
exactly as on Linux — no new sharing mechanism is needed anywhere.
