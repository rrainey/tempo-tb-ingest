"""Step 9: presence & return detection — the full transition matrix.

Pure logic on a simulated clock: no radios, no sleeping. Events are captured
from a real EventBus subscription and asserted as (type, key-fields) tuples.
"""

import asyncio
from datetime import UTC, datetime, timedelta

from tempo_tb_ingest.events import Envelope, EventBus
from tempo_tb_ingest.presence import (
    DeviceState,
    PresenceTracker,
    device_id_from_name,
)
from tempo_tb_ingest.scanner import Sighting

T0 = datetime(2026, 7, 8, 12, 0, 0, tzinfo=UTC)

LOST_AFTER = 90.0
ABSENT_AFTER = 600.0
FLOOR = -75

MAC_A = "DC:BD:F1:0D:F1:D9"
MAC_B = "AA:BB:CC:DD:EE:FF"


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


def sighting(
    t: float, mac: str = MAC_A, name: str | None = "Tempo-BT-0001", rssi: int = -60
) -> Sighting:
    return Sighting(mac=mac, name=name, rssi=rssi, ts=at(t))


class Harness:
    def __init__(self) -> None:
        self.bus = EventBus()
        self.subscription = self.bus.subscribe(queue_size=4096)
        self.returned: list[str] = []
        self.tracker = PresenceTracker(
            self.bus,
            rssi_floor_dbm=FLOOR,
            lost_after_s=LOST_AFTER,
            absent_after_s=ABSENT_AFTER,
            on_returned=self.returned.append,
        )

    def events(self) -> list[Envelope]:
        self.bus.close()

        async def drain() -> list[Envelope]:
            return [e async for e in self.subscription]

        return asyncio.run(drain())

    def types(self) -> list[str]:
        return [e.type for e in self.events()]


class TestDeviceIdParsing:
    def test_valid(self) -> None:
        assert device_id_from_name("Tempo-BT-0001") == "0001"
        assert device_id_from_name("Tempo-BT-ABCD") == "ABCD"

    def test_bare_and_malformed(self) -> None:
        assert device_id_from_name("Tempo-BT") is None
        assert device_id_from_name("Tempo-BT-") is None
        assert device_id_from_name("Tempo-BT-001") is None
        assert device_id_from_name("Tempo-BT-00123") is None
        assert device_id_from_name("Fitbit") is None
        assert device_id_from_name(None) is None


class TestFirstSighting:
    def test_new_device_triggers_backlog_return(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0))
        events = h.events()
        assert [e.type for e in events] == ["device.new", "device.returned"]
        assert events[0].data["id"] == "0001"
        assert events[1].data["absent_for_s"] is None  # first-ever
        assert h.returned == ["0001"]

    def test_weak_first_sighting_ignored(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0, rssi=-90))
        assert h.types() == []
        assert h.tracker.devices() == []


class TestSeenThrottle:
    def test_rapid_sightings_throttled(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0))
        h.tracker.observe(sighting(0.3))
        h.tracker.observe(sighting(0.6))
        h.tracker.observe(sighting(1.2))  # >= 1 s since the last seen event
        types = h.types()
        assert types.count("device.seen") == 1
        assert types == ["device.new", "device.returned", "device.seen"]


class TestWeakSightings:
    def test_weak_updates_display_only(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0))
        h.tracker.observe(sighting(5, rssi=-90))
        record = h.tracker.devices()[0]
        assert record.last_weak == at(5)
        assert record.last_seen == at(0)  # state untouched
        assert h.types() == ["device.new", "device.returned"]

    def test_weak_does_not_prevent_away(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0))
        h.tracker.observe(sighting(85, rssi=-90))  # flapping at the floor
        h.tracker.sweep(at(LOST_AFTER))
        assert h.tracker.devices()[0].state is DeviceState.AWAY


class TestAwayAndReturn:
    def test_sweep_moves_present_to_away(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0))
        h.tracker.sweep(at(LOST_AFTER))
        record = h.tracker.devices()[0]
        assert record.state is DeviceState.AWAY
        assert record.away_since == at(0)
        events = h.events()
        assert events[-1].type == "device.away"
        assert events[-1].data["away_since"] == "2026-07-08T12:00:00.000Z"

    def test_sweep_idempotent(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0))
        h.tracker.sweep(at(LOST_AFTER))
        h.tracker.sweep(at(LOST_AFTER + 30))
        assert h.types().count("device.away") == 1

    def test_short_blip_is_not_a_return(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0))
        h.tracker.sweep(at(LOST_AFTER))
        h.tracker.observe(sighting(300))  # back before absent_after
        record = h.tracker.devices()[0]
        assert record.state is DeviceState.PRESENT
        assert h.types().count("device.returned") == 1  # only the first-ever
        assert h.returned == ["0001"]

    def test_full_absence_is_a_return(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0))
        h.tracker.sweep(at(LOST_AFTER))
        h.tracker.observe(sighting(700))
        events = [e for e in h.events() if e.type == "device.returned"]
        assert len(events) == 2
        assert events[1].data["absent_for_s"] == 700.0
        assert h.returned == ["0001", "0001"]

    def test_exact_boundary_returns(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0))
        h.tracker.sweep(at(LOST_AFTER))
        h.tracker.observe(sighting(ABSENT_AFTER))
        assert h.returned == ["0001", "0001"]

    def test_quiescent_after_harvest_until_next_cycle(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0))
        h.tracker.mark_harvested("0001", at(10))
        for t in (20, 40, 60):
            h.tracker.observe(sighting(t))
        assert h.returned == ["0001"]  # no re-trigger while continuously present
        assert h.tracker.devices()[0].last_harvested == at(10)


class TestRebootContinuity:
    def test_mid_visit_power_cycle_keeps_identity(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0, mac=MAC_A))
        h.tracker.observe(sighting(10, mac=MAC_B))  # rebooted: new MAC, same name
        records = h.tracker.devices()
        assert len(records) == 1
        assert records[0].mac == MAC_B
        assert records[0].conflicted is False
        types = h.types()
        assert "device.identity_conflict" not in types
        assert types.count("device.new") == 1

    def test_return_after_absence_with_new_mac(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0, mac=MAC_A))
        h.tracker.sweep(at(LOST_AFTER))
        h.tracker.observe(sighting(700, mac=MAC_B))
        assert h.returned == ["0001", "0001"]  # continuity: same id


class TestIdentityConflict:
    def test_flip_back_within_window_is_conflict(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0, mac=MAC_A))
        h.tracker.observe(sighting(5, mac=MAC_B))
        h.tracker.observe(sighting(10, mac=MAC_A))  # flip back: two devices
        record = h.tracker.devices()[0]
        assert record.conflicted is True
        conflicts = [e for e in h.events() if e.type == "device.identity_conflict"]
        assert len(conflicts) == 1
        assert set(conflicts[0].data["macs"]) == {MAC_A, MAC_B}

    def test_conflict_blocks_harvest_trigger_even_across_absence(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0, mac=MAC_A))
        h.tracker.observe(sighting(5, mac=MAC_B))
        h.tracker.observe(sighting(10, mac=MAC_A))
        h.tracker.sweep(at(10 + LOST_AFTER))
        t_return = 10 + LOST_AFTER + ABSENT_AFTER
        h.tracker.observe(sighting(t_return, mac=MAC_A))
        # silence does not clear a conflict: the return is announced (loud)
        # but does not queue a harvest
        assert h.tracker.devices()[0].conflicted is True
        assert h.returned == ["0001"]  # only the pre-conflict first sighting

        # recovery: sustained single-MAC presence clears it, then the *next*
        # away/return cycle harvests normally
        h.tracker.observe(sighting(t_return + 40, mac=MAC_A))
        assert h.tracker.devices()[0].conflicted is False
        h.tracker.sweep(at(t_return + 40 + LOST_AFTER))
        h.tracker.observe(sighting(t_return + 40 + LOST_AFTER + ABSENT_AFTER, mac=MAC_A))
        assert h.returned == ["0001", "0001"]
        assert h.types().count("device.returned") == 3

    def test_conflict_clears_after_quiet_window(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0, mac=MAC_A))
        h.tracker.observe(sighting(5, mac=MAC_B))
        h.tracker.observe(sighting(10, mac=MAC_A))
        assert h.tracker.devices()[0].conflicted is True
        # one MAC keeps advertising alone past the window
        h.tracker.observe(sighting(50, mac=MAC_A))
        assert h.tracker.devices()[0].conflicted is False

    def test_slow_mac_change_is_not_conflict(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0, mac=MAC_A))
        h.tracker.observe(sighting(5, mac=MAC_B))
        h.tracker.observe(sighting(60, mac=MAC_A))  # flip back after window
        assert h.tracker.devices()[0].conflicted is False


class TestUnprovisioned:
    def test_bare_name_surfaces_but_never_tracks(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0, name="Tempo-BT"))
        assert h.tracker.devices() == []
        assert [u.mac for u in h.tracker.unprovisioned()] == [MAC_A]
        assert h.types() == ["device.provisioning_needed"]
        assert h.returned == []

    def test_provisioning_event_throttled(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0, name="Tempo-BT"))
        h.tracker.observe(sighting(30, name="Tempo-BT"))
        h.tracker.observe(sighting(700, name="Tempo-BT"))  # past repeat interval
        assert h.types().count("device.provisioning_needed") == 2

    def test_unnamed_sighting_ignored(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0, name=None))
        assert h.tracker.devices() == []
        assert h.tracker.unprovisioned() == []
        assert h.types() == []


class TestPruning:
    def test_long_gone_device_pruned(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0))
        h.tracker.sweep(at(LOST_AFTER))
        h.tracker.sweep(at(24 * 3600 + 1))
        assert h.tracker.devices() == []
        assert h.types()[-1] == "device.lost"

    def test_reappearance_after_prune_is_new(self) -> None:
        h = Harness()
        h.tracker.observe(sighting(0))
        h.tracker.sweep(at(LOST_AFTER))
        h.tracker.sweep(at(24 * 3600 + 1))
        h.tracker.observe(sighting(25 * 3600))
        assert h.types().count("device.new") == 2
        assert h.returned == ["0001", "0001"]
