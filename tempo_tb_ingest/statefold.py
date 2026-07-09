"""Fold the event stream into the §6.1 snapshot structure.

Two uses:
- replay mode: the API serves state folded from a recording, so the dashboard
  cannot tell replay from live (design §3.8);
- the reference reducer: contract tests assert the daemon's live snapshot and
  the fold agree on the wire format.

The live daemon (step 15) composes its snapshot from real components where
they are richer (e.g. ``sessions_known`` from the store); the fold
approximates those from events alone.
"""

from datetime import datetime
from typing import Any

from tempo_tb_ingest import __version__
from tempo_tb_ingest.events import Envelope, format_ts


class StateFold:
    """Accumulates envelopes; renders the §6.1 snapshot at any moment."""

    def __init__(
        self,
        *,
        version: str = __version__,
        adapters: dict[str, Any] | None = None,
    ) -> None:
        self._version = version
        self._adapters = adapters or {"scan": None, "transfer": []}
        self._started_at: str | None = None
        self._scanning = True
        self._warnings: list[str] = []
        self._devices: dict[str, dict[str, Any]] = {}
        self._unprovisioned: dict[str, dict[str, Any]] = {}
        self._queue: list[dict[str, Any]] = []
        self._active_job: dict[str, Any] | None = None
        self._totals = {
            "sessions_stored": 0,
            "bytes_stored": 0,
            "harvests_completed": 0,
            "failures": 0,
        }
        self.last_seq = 0
        self.last_ts: datetime | None = None

    # ------------------------------------------------------------------ #

    def apply(self, env: Envelope) -> None:
        if env.seq > 0:
            self.last_seq = max(self.last_seq, env.seq)
        self.last_ts = env.ts
        data = env.data
        handler = getattr(self, "_on_" + env.type.replace(".", "_"), None)
        if handler is not None:
            handler(data, env)

    def _device(self, device_id: str) -> dict[str, Any]:
        return self._devices.setdefault(
            device_id,
            {
                "id": device_id,
                "name": None,
                "folder": f"TempoBT-{device_id}",
                "mac": None,
                "jumper": None,
                "is_lo": False,
                "state": "PRESENT",
                "rssi": None,
                "last_seen": None,
                "away_since": None,
                "sessions_known": 0,
                "provisioning_needed": False,
                "conflicted": False,
                "truncated": False,
            },
        )

    # -- event handlers ---------------------------------------------------- #

    def _on_daemon_started(self, data: dict[str, Any], env: Envelope) -> None:
        self._version = data.get("version", self._version)
        self._started_at = format_ts(env.ts)
        self._scanning = True

    def _on_daemon_stopping(self, data: dict[str, Any], env: Envelope) -> None:
        self._scanning = False

    def _on_scanner_degraded(self, data: dict[str, Any], env: Envelope) -> None:
        self._scanning = False
        warning = f"scanner degraded: {data.get('reason', '?')}"
        if warning not in self._warnings:
            self._warnings.append(warning)

    def _on_scanner_recovered(self, data: dict[str, Any], env: Envelope) -> None:
        self._scanning = True
        self._warnings = [w for w in self._warnings if not w.startswith("scanner degraded")]

    def _sighting(self, data: dict[str, Any], env: Envelope) -> None:
        record = self._device(data["id"])
        record.update(
            name=data["name"],
            mac=data["mac"],
            rssi=data["rssi"],
            state="PRESENT",
            away_since=None,
            last_seen=format_ts(env.ts),
        )

    _on_device_seen = _sighting
    _on_device_new = _sighting

    def _on_device_away(self, data: dict[str, Any], env: Envelope) -> None:
        record = self._device(data["id"])
        record["state"] = "AWAY"
        record["away_since"] = data["away_since"]

    def _on_device_returned(self, data: dict[str, Any], env: Envelope) -> None:
        record = self._device(data["id"])
        record["state"] = "PRESENT"
        record["away_since"] = None

    def _on_device_lost(self, data: dict[str, Any], env: Envelope) -> None:
        self._devices.pop(data["id"], None)

    def _on_device_provisioning_needed(self, data: dict[str, Any], env: Envelope) -> None:
        self._unprovisioned[data["mac"]] = {
            "id": None,
            "name": data["name"],
            "mac": data["mac"],
            "provisioning_needed": True,
            "last_seen": format_ts(env.ts),
        }

    def _on_device_identity_conflict(self, data: dict[str, Any], env: Envelope) -> None:
        record = self._device(data["id"])
        record["conflicted"] = True
        warning = f"identity conflict: id {data['id']} at {', '.join(data.get('macs', []))}"
        if warning not in self._warnings:
            self._warnings.append(warning)

    def _on_harvest_queued(self, data: dict[str, Any], env: Envelope) -> None:
        if not any(q["id"] == data["id"] for q in self._queue):
            self._queue.append({"id": data["id"], "queued_at": format_ts(env.ts)})

    def _on_harvest_started(self, data: dict[str, Any], env: Envelope) -> None:
        self._queue = [q for q in self._queue if q["id"] != data["id"]]
        self._active_job = {
            "id": data["id"],
            "state": "CONNECTING",
            "session_key": None,
            "file_index": None,
            "file_total": None,
            "bytes_done": 0,
            "bytes_total": None,
            "rate_bps": 0.0,
        }

    def _on_harvest_session_list(self, data: dict[str, Any], env: Envelope) -> None:
        if self._active_job is not None and self._active_job["id"] == data["id"]:
            self._active_job["state"] = "ENUMERATING"

    def _on_harvest_truncated(self, data: dict[str, Any], env: Envelope) -> None:
        self._device(data["id"])["truncated"] = True
        warning = f"session list truncated on {data['id']}"
        if warning not in self._warnings:
            self._warnings.append(warning)

    def _on_transfer_started(self, data: dict[str, Any], env: Envelope) -> None:
        if self._active_job is not None and self._active_job["id"] == data["id"]:
            self._active_job.update(
                state="DOWNLOADING",
                session_key=data["session_key"],
                file_index=data["file_index"],
                file_total=data["file_total"],
                bytes_done=data["resumed_from"],
                bytes_total=None,
                rate_bps=0.0,
            )

    def _on_transfer_progress(self, data: dict[str, Any], env: Envelope) -> None:
        if self._active_job is not None and self._active_job["id"] == data["id"]:
            self._active_job.update(
                bytes_done=data["bytes_done"],
                bytes_total=data["bytes_total"],
                rate_bps=data["rate_bps"],
            )

    def _on_store_session_added(self, data: dict[str, Any], env: Envelope) -> None:
        self._totals["sessions_stored"] += 1
        self._totals["bytes_stored"] += data["size"]
        record = self._device(data["id"])
        record["sessions_known"] += 1
        if data.get("jumper"):
            record["jumper"] = data["jumper"]

    def _on_harvest_completed(self, data: dict[str, Any], env: Envelope) -> None:
        self._totals["harvests_completed"] += 1
        if self._active_job is not None and self._active_job["id"] == data["id"]:
            self._active_job = None

    def _on_harvest_failed(self, data: dict[str, Any], env: Envelope) -> None:
        self._totals["failures"] += 1
        if self._active_job is not None and self._active_job["id"] == data["id"]:
            self._active_job = None

    # ------------------------------------------------------------------ #

    def snapshot(self, *, now: datetime | None = None) -> dict[str, Any]:
        """The §6.1 wire structure (v/seq/ts + sections)."""
        ts = now or self.last_ts
        return {
            "v": 1,
            "seq": self.last_seq,
            "ts": format_ts(ts) if ts else None,
            "daemon": {
                "version": self._version,
                "started_at": self._started_at,
                "adapters": self._adapters,
                "scanning": self._scanning,
                "warnings": list(self._warnings),
            },
            "devices": sorted(self._devices.values(), key=lambda d: d["id"] or "")
            + sorted(self._unprovisioned.values(), key=lambda d: d["mac"]),
            "queue": list(self._queue),
            "active_job": dict(self._active_job) if self._active_job else None,
            "totals": dict(self._totals),
        }
