"""Presence table and return detection (design §3.3/§3.4).

Keyed by device id (the 4-char name suffix). The MAC is a transient
correlator: it changes across power cycles, so a mid-visit reboot (new MAC,
same id) keeps continuity. Sightings that cannot be attributed — no name yet,
or the bare unprovisioned ``Tempo-BT`` name — never drive state.

State machine per device:

    (no record) --sighting--> PRESENT  [device.new + device.returned(None)]
    PRESENT --no sighting for lost_after--> AWAY        [device.away]
    AWAY --sighting within absent_after--> PRESENT      [quiet: short blip]
    AWAY --sighting after >= absent_after--> PRESENT    [device.returned]
    AWAY --no sighting for prune_after--> (pruned)      [device.lost]

``device.returned`` is the harvest trigger (``on_returned`` callback).
Quiescence after a harvest is inherent: another trigger requires a full
AWAY >= absent_after cycle.

Identity conflicts (duplicate suffix in the fleet): a *flip-back* between
MACs — sightings going A → B → A within ``conflict_window_s`` — cannot be a
reboot (a rebooted device never resumes its old address) and marks the id
conflicted: no harvest triggers until the flapping stops for a full window.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from tempo_tb_ingest.events import (
    DeviceAway,
    DeviceIdentityConflict,
    DeviceLost,
    DeviceNew,
    DeviceProvisioningNeeded,
    DeviceReturned,
    DeviceSeen,
    EventBus,
)
from tempo_tb_ingest.scanner import TEMPO_NAME_PREFIX, Sighting

DEVICE_ID_PATTERN = TEMPO_NAME_PREFIX + "-"

SEEN_THROTTLE_S = 1.0
DEFAULT_PRUNE_AFTER_S = 24 * 3600.0
DEFAULT_CONFLICT_WINDOW_S = 30.0
PROVISIONING_REPEAT_S = 600.0


def device_id_from_name(name: str | None) -> str | None:
    """``Tempo-BT-0001`` → ``0001``; bare/foreign names → None."""
    if name is None or not name.startswith(DEVICE_ID_PATTERN):
        return None
    suffix = name.removeprefix(DEVICE_ID_PATTERN)
    return suffix if len(suffix) == 4 else None


class DeviceState(StrEnum):
    PRESENT = "PRESENT"
    AWAY = "AWAY"


@dataclass
class DeviceRecord:
    id: str
    name: str
    mac: str
    rssi: int
    first_seen: datetime
    last_seen: datetime
    state: DeviceState = DeviceState.PRESENT
    away_since: datetime | None = None
    last_weak: datetime | None = None  # sub-floor sightings: display only
    last_harvested: datetime | None = None
    conflicted: bool = False
    # identity-conflict detection: the previous MAC and when we left it
    _prior_mac: str | None = None
    _prior_mac_left: datetime | None = None
    _last_seen_event: datetime | None = None
    _macs_in_conflict: set[str] = field(default_factory=set)


@dataclass
class UnprovisionedRecord:
    mac: str
    name: str
    last_seen: datetime
    last_event: datetime


class PresenceTracker:
    """Consumes Sightings; owns all presence state; emits presence events."""

    def __init__(
        self,
        bus: EventBus,
        *,
        rssi_floor_dbm: int,
        lost_after_s: float,
        absent_after_s: float,
        prune_after_s: float = DEFAULT_PRUNE_AFTER_S,
        conflict_window_s: float = DEFAULT_CONFLICT_WINDOW_S,
        on_returned: Callable[[str], None] | None = None,
    ) -> None:
        self._bus = bus
        self._rssi_floor = rssi_floor_dbm
        self._lost_after_s = lost_after_s
        self._absent_after_s = absent_after_s
        self._prune_after_s = prune_after_s
        self._conflict_window_s = conflict_window_s
        self._on_returned = on_returned
        self._devices: dict[str, DeviceRecord] = {}
        self._unprovisioned: dict[str, UnprovisionedRecord] = {}

    # -- inputs ---------------------------------------------------------------

    def observe(self, sighting: Sighting) -> None:
        """Feed one filtered advertisement sighting."""
        device_id = device_id_from_name(sighting.name)
        if device_id is None:
            if sighting.name is not None and sighting.name.startswith(TEMPO_NAME_PREFIX):
                self._observe_unprovisioned(sighting)
            return  # unnamed or foreign: never drives state

        record = self._devices.get(device_id)
        if record is not None and sighting.rssi < self._rssi_floor:
            record.last_weak = sighting.ts  # display only; no state transition
            return
        if record is None:
            if sighting.rssi < self._rssi_floor:
                return  # too weak to even establish the device
            self._first_sighting(device_id, sighting)
            return
        self._track_identity_conflict(record, sighting)
        self._accepted_sighting(record, sighting)

    def sweep(self, now: datetime) -> None:
        """Apply time-based transitions; call periodically (idempotent)."""
        for record in list(self._devices.values()):
            silent_s = (now - record.last_seen).total_seconds()
            if record.state is DeviceState.PRESENT and silent_s >= self._lost_after_s:
                record.state = DeviceState.AWAY
                record.away_since = record.last_seen
                self._bus.publish(DeviceAway(id=record.id, away_since=record.last_seen))
            elif record.state is DeviceState.AWAY and silent_s >= self._prune_after_s:
                del self._devices[record.id]
                self._bus.publish(DeviceLost(id=record.id))

    def mark_harvested(self, device_id: str, now: datetime) -> None:
        record = self._devices.get(device_id)
        if record is not None:
            record.last_harvested = now

    # -- snapshot (API, step 14) ----------------------------------------------

    def devices(self) -> list[DeviceRecord]:
        return list(self._devices.values())

    def unprovisioned(self) -> list[UnprovisionedRecord]:
        return list(self._unprovisioned.values())

    # -- internals --------------------------------------------------------------

    def _first_sighting(self, device_id: str, sighting: Sighting) -> None:
        assert sighting.name is not None
        record = DeviceRecord(
            id=device_id,
            name=sighting.name,
            mac=sighting.mac,
            rssi=sighting.rssi,
            first_seen=sighting.ts,
            last_seen=sighting.ts,
        )
        record._last_seen_event = sighting.ts
        self._devices[device_id] = record
        self._bus.publish(
            DeviceNew(id=device_id, mac=sighting.mac, name=sighting.name, rssi=sighting.rssi)
        )
        # first-ever sighting counts as a return: harvest the unknown backlog
        self._trigger_return(record, absent_for_s=None)

    def _accepted_sighting(self, record: DeviceRecord, sighting: Sighting) -> None:
        assert sighting.name is not None
        absent_for_s = (sighting.ts - record.last_seen).total_seconds()
        was_away = record.state is DeviceState.AWAY

        record.name = sighting.name
        record.mac = sighting.mac
        record.rssi = sighting.rssi
        record.last_seen = sighting.ts
        record.state = DeviceState.PRESENT
        record.away_since = None

        if record._last_seen_event is None or (
            (sighting.ts - record._last_seen_event).total_seconds() >= SEEN_THROTTLE_S
        ):
            record._last_seen_event = sighting.ts
            self._bus.publish(
                DeviceSeen(id=record.id, mac=sighting.mac, name=sighting.name, rssi=sighting.rssi)
            )

        if was_away and absent_for_s >= self._absent_after_s:
            self._trigger_return(record, absent_for_s=absent_for_s)

    def _trigger_return(self, record: DeviceRecord, absent_for_s: float | None) -> None:
        self._bus.publish(DeviceReturned(id=record.id, absent_for_s=absent_for_s))
        if record.conflicted:
            return  # blocked from harvest until the conflict clears
        if self._on_returned is not None:
            self._on_returned(record.id)

    def _track_identity_conflict(self, record: DeviceRecord, sighting: Sighting) -> None:
        if sighting.mac == record.mac:
            self._maybe_clear_conflict(record, sighting.ts)
            return
        # MAC changed. A reboot moves A -> B and never returns to A; a
        # flip-back to the address we recently left means two devices share
        # this id.
        flip_back = (
            record._prior_mac == sighting.mac
            and record._prior_mac_left is not None
            and (sighting.ts - record._prior_mac_left).total_seconds() <= self._conflict_window_s
        )
        record._prior_mac = record.mac
        record._prior_mac_left = sighting.ts
        if flip_back:
            macs = {record.mac, sighting.mac} | record._macs_in_conflict
            if not record.conflicted or macs != record._macs_in_conflict:
                record.conflicted = True
                record._macs_in_conflict = macs
                self._bus.publish(DeviceIdentityConflict(id=record.id, macs=sorted(macs)))

    def _maybe_clear_conflict(self, record: DeviceRecord, now: datetime) -> None:
        # Clearing requires *sustained single-MAC presence*: a full conflict
        # window on one address with the device continuously in range. Mere
        # silence (absence) never clears — a conflicted id stays blocked
        # through an away/return cycle until it demonstrably stops flapping.
        if (
            record.conflicted
            and record._prior_mac_left is not None
            and (now - record._prior_mac_left).total_seconds() > self._conflict_window_s
            and (now - record.last_seen).total_seconds() <= self._lost_after_s
        ):
            record.conflicted = False
            record._macs_in_conflict = set()

    def _observe_unprovisioned(self, sighting: Sighting) -> None:
        assert sighting.name is not None
        known = self._unprovisioned.get(sighting.mac)
        if known is None or (
            (sighting.ts - known.last_event).total_seconds() >= PROVISIONING_REPEAT_S
        ):
            self._unprovisioned[sighting.mac] = UnprovisionedRecord(
                mac=sighting.mac,
                name=sighting.name,
                last_seen=sighting.ts,
                last_event=sighting.ts,
            )
            self._bus.publish(DeviceProvisioningNeeded(mac=sighting.mac, name=sighting.name))
        else:
            known.last_seen = sighting.ts
