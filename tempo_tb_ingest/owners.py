"""The device ownership registry: device-owners.json (design §3.12).

User-maintained JSON at ``<staging_root>/device-owners.json`` — the one piece
of jump context logs cannot provide: who wears each device, and who organizes
the load (the default formation base for analysis).

    [
      { "deviceName": "Tempo-BT-0001", "jumperName": "riley", "isLoadOrganizer": true },
      { "deviceName": "Tempo-BT-0002", "jumperName": "russ" }
    ]

Semantics (design §3.12):
- hot-reload on mtime change, checked at each lookup — attribution is bound
  at *harvest* time, so edits take effect on the next harvest;
- an invalid file is a loud ``owners.error`` and the last good copy stays in
  use — the registry never guesses and never blocks harvesting;
- a device with no entry simply resolves to None (stored unattributed).
"""

import json
import re
from dataclasses import dataclass
from pathlib import Path

from tempo_tb_ingest.events import EventBus, OwnersError, OwnersReloaded
from tempo_tb_ingest.presence import device_id_from_name

#: jumperName becomes a test-data directory name — restrict accordingly
#: (matches existing jumper dirs: riley, russ, divyatej_dt, billy)
JUMPER_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


@dataclass(frozen=True)
class OwnerEntry:
    device_id: str  # 4-char suffix from deviceName
    device_name: str
    jumper_name: str
    is_load_organizer: bool


class OwnersRegistry:
    """Lazy hot-reloading view of device-owners.json, keyed by device id."""

    def __init__(self, path: Path, bus: EventBus | None = None) -> None:
        self._path = path
        self._bus = bus
        self._entries: dict[str, OwnerEntry] = {}
        self._loaded_mtime: float | None = None
        self._error_reported_for: float | None = None  # mtime (or -1 for missing)

    @property
    def path(self) -> Path:
        return self._path

    def lookup(self, device_id: str) -> OwnerEntry | None:
        self._maybe_reload()
        return self._entries.get(device_id)

    def entries(self) -> list[OwnerEntry]:
        self._maybe_reload()
        return list(self._entries.values())

    # -- internals ------------------------------------------------------------

    def _maybe_reload(self) -> None:
        try:
            mtime = self._path.stat().st_mtime
        except OSError:
            if self._error_reported_for != -1.0:
                self._error_reported_for = -1.0
                self._publish_error("registry file not found")
            return
        if mtime == self._loaded_mtime:
            return
        try:
            entries = _parse(self._path)
        except ValueError as exc:
            # keep the last good copy; report each bad revision once
            if self._error_reported_for != mtime:
                self._error_reported_for = mtime
                self._publish_error(str(exc))
            self._loaded_mtime = mtime  # don't re-parse the same bad file every lookup
            return
        self._entries = entries
        self._loaded_mtime = mtime
        self._error_reported_for = None
        if self._bus is not None:
            self._bus.publish(OwnersReloaded(entries=len(entries), path=str(self._path)))

    def _publish_error(self, reason: str) -> None:
        if self._bus is not None:
            self._bus.publish(OwnersError(reason=reason, path=str(self._path)))


def _parse(path: Path) -> dict[str, OwnerEntry]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"unparseable JSON: {exc}") from exc
    if not isinstance(raw, list):
        raise ValueError("top level must be a JSON array of records")

    entries: dict[str, OwnerEntry] = {}
    for index, record in enumerate(raw):
        where = f"record {index}"
        if not isinstance(record, dict):
            raise ValueError(f"{where}: not an object")
        unknown = set(record) - {"deviceName", "jumperName", "isLoadOrganizer"}
        if unknown:
            raise ValueError(f"{where}: unknown field(s) {sorted(unknown)}")
        device_name = record.get("deviceName")
        jumper_name = record.get("jumperName")
        is_lo = record.get("isLoadOrganizer", False)
        if not isinstance(device_name, str) or not isinstance(jumper_name, str):
            raise ValueError(f"{where}: deviceName and jumperName must be strings")
        if not isinstance(is_lo, bool):
            raise ValueError(f"{where}: isLoadOrganizer must be a boolean")
        device_id = device_id_from_name(device_name)
        if device_id is None:
            raise ValueError(
                f"{where}: deviceName {device_name!r} must be 'Tempo-BT-' + 4-char suffix"
            )
        if not JUMPER_NAME_RE.match(jumper_name):
            raise ValueError(
                f"{where}: jumperName {jumper_name!r} is not a valid test-data directory name"
            )
        if device_id in entries:
            raise ValueError(f"{where}: duplicate deviceName suffix {device_id!r}")
        entries[device_id] = OwnerEntry(
            device_id=device_id,
            device_name=device_name,
            jumper_name=jumper_name,
            is_load_organizer=is_lo,
        )
    return entries
