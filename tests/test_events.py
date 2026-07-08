"""Step 3: event schemas (golden wire fixtures) and bus semantics.

Golden fixtures live in tests/fixtures/events/<type>.json — one per event
type. They lock the wire format: any schema change shows up as a fixture
diff. Regenerate deliberately with:  REGEN_FIXTURES=1 uv run pytest tests/test_events.py
"""

import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tempo_tb_ingest import events as ev

FIXTURES = Path(__file__).parent / "fixtures" / "events"

FIXED_TS = datetime(2026, 7, 8, 17, 3, 22, 114000, tzinfo=UTC)
FIXED_SEQ = 42

# One fully-populated exemplar per event type; the golden fixtures are the
# serialized envelopes of exactly these.
EXEMPLARS: list[ev.EventData] = [
    ev.DaemonStarted(version="0.1.0", config={"detection": {"absent_after_s": 600.0}}),
    ev.DaemonStopping(reason="SIGTERM"),
    ev.ScannerDegraded(reason="org.bluez.Error.NotReady"),
    ev.ScannerRecovered(outage_s=4.2),
    ev.DeviceSeen(id="0001", mac="DC:BD:F1:0D:F1:D9", name="Tempo-BT-0001", rssi=-58),
    ev.DeviceNew(id="0002", mac="C8:43:CA:EB:FE:6D", name="Tempo-BT-0002", rssi=-71),
    ev.DeviceAway(id="0001", away_since=FIXED_TS),
    ev.DeviceReturned(id="0001", absent_for_s=1287.5),
    ev.DeviceLost(id="0002"),
    ev.DeviceProvisioningNeeded(mac="F0:11:22:33:44:55", name="Tempo-BT"),
    ev.DeviceIdentityConflict(id="0001", macs=["DC:BD:F1:0D:F1:D9", "AA:BB:CC:DD:EE:FF"]),
    ev.HarvestQueued(id="0001", attempt=1),
    ev.HarvestStarted(id="0001", attempt=1),
    ev.HarvestSessionList(id="0001", count=26, new_count=3, truncated=False),
    ev.HarvestTruncated(id="0001"),
    ev.TransferStarted(
        id="0001", session_key="20260705/1CDD8C18", file_index=1, file_total=3, resumed_from=0
    ),
    ev.TransferProgress(
        id="0001",
        session_key="20260705/1CDD8C18",
        bytes_done=1310720,
        bytes_total=2922782,
        rate_bps=43000.0,
    ),
    ev.TransferCompleted(
        id="0001",
        session_key="20260705/1CDD8C18",
        bytes=2922782,
        sha256="3dea28ef25f25f42ca7ba484b239b377c6d47d7fd46c0e108cd08937f03de858",
        duration_s=70.3,
    ),
    ev.TransferFailed(
        id="0001", session_key="20260705/1CDD8C18", reason="disconnected", resumable=True
    ),
    ev.StoreSessionAdded(
        id="0001",
        session_key="20260705/1CDD8C18",
        path="TempoBT-0001/logs/20260705/1CDD8C18/flight.txt",
        size=2922782,
        sha256="3dea28ef25f25f42ca7ba484b239b377c6d47d7fd46c0e108cd08937f03de858",
        jumper="riley",
    ),
    ev.StoreDuplicateHash(
        id="0002",
        session_key="20260705/00BAF6AB",
        sha256="3dea28ef25f25f42ca7ba484b239b377c6d47d7fd46c0e108cd08937f03de858",
        duplicate_of="0001/20260705/1CDD8C18",
    ),
    ev.StoreError(id="0001", session_key="20260705/1CDD8C18", reason="disk full"),
    ev.OwnersReloaded(entries=3, path="/data/device-data/device-owners.json"),
    ev.OwnersError(reason="duplicate deviceName", path="/data/device-data/device-owners.json"),
    ev.OwnersUnmapped(id="0004", name="Tempo-BT-0004"),
    ev.HarvestCompleted(id="0001", sessions_downloaded=3, bytes=11979117, duration_s=284.0),
    ev.HarvestFailed(id="0001", reason="connect timeout", attempt=2, will_retry=True),
    ev.StreamGap(dropped_count=17),
]


def fixture_path(event_type: str) -> Path:
    return FIXTURES / f"{event_type}.json"


def envelope_of(data: ev.EventData) -> ev.Envelope:
    return ev.Envelope(seq=FIXED_SEQ, ts=FIXED_TS, type=data.TYPE, data=data.model_dump())


class TestGoldenFixtures:
    def test_exemplars_cover_registry_exactly(self) -> None:
        exemplar_types = {e.TYPE for e in EXEMPLARS}
        assert exemplar_types == set(ev.EVENT_TYPES), (
            "every registered event type needs an exemplar (and vice versa)"
        )

    @pytest.mark.parametrize("data", EXEMPLARS, ids=lambda d: d.TYPE)
    def test_wire_format_locked(self, data: ev.EventData) -> None:
        env = envelope_of(data)
        wire = json.dumps(json.loads(env.to_json()), indent=2, sort_keys=True) + "\n"
        path = fixture_path(data.TYPE)
        if os.environ.get("REGEN_FIXTURES"):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(wire)
        assert path.is_file(), f"missing golden fixture {path.name}; see module docstring"
        assert path.read_text() == wire, (
            f"wire format changed for {data.TYPE}; if intentional, regenerate fixtures"
        )

    @pytest.mark.parametrize("data", EXEMPLARS, ids=lambda d: d.TYPE)
    def test_fixture_round_trips(self, data: ev.EventData) -> None:
        env = ev.Envelope.from_json(fixture_path(data.TYPE).read_text())
        assert env.type == data.TYPE
        assert env.payload() == data

    def test_no_orphan_fixtures(self) -> None:
        on_disk = {p.stem for p in FIXTURES.glob("*.json")}
        assert on_disk == set(ev.EVENT_TYPES)


class TestEnvelope:
    def test_ts_format_is_millisecond_z(self) -> None:
        assert ev.format_ts(FIXED_TS) == "2026-07-08T17:03:22.114Z"

    def test_unknown_type_rejected(self) -> None:
        env = ev.Envelope(seq=1, ts=FIXED_TS, type="nope.nope", data={})
        with pytest.raises(ev.EventError, match="unknown event type"):
            env.payload()

    def test_malformed_payload_rejected(self) -> None:
        env = ev.Envelope(seq=1, ts=FIXED_TS, type="device.lost", data={"wrong": 1})
        with pytest.raises(ev.EventError, match="malformed"):
            env.payload()

    def test_from_json_rejects_garbage(self) -> None:
        with pytest.raises(ev.EventError):
            ev.Envelope.from_json("not json at all")


class TestBus:
    def test_seq_monotonic_across_concurrent_publishers(self) -> None:
        async def scenario() -> list[int]:
            bus = ev.EventBus()
            sub = bus.subscribe(queue_size=1000)

            async def publisher(n: int) -> None:
                for _ in range(n):
                    bus.publish(ev.DeviceLost(id="0001"))
                    await asyncio.sleep(0)

            await asyncio.gather(*(publisher(50) for _ in range(4)))
            bus.close()
            return [env.seq async for env in sub]

        seqs = asyncio.run(scenario())
        assert seqs == list(range(1, 201))

    def test_fan_out_order_preserved_per_subscriber(self) -> None:
        async def scenario() -> tuple[list[int], list[int]]:
            bus = ev.EventBus()
            a, b = bus.subscribe(), bus.subscribe()
            for _ in range(10):
                bus.publish(ev.DeviceLost(id="0001"))
            bus.close()
            return [e.seq async for e in a], [e.seq async for e in b]

        got_a, got_b = asyncio.run(scenario())
        assert got_a == got_b == list(range(1, 11))

    def test_slow_subscriber_gets_gap_not_blocking(self) -> None:
        async def scenario() -> list[ev.Envelope]:
            bus = ev.EventBus(queue_size=4)
            sub = bus.subscribe()
            for _ in range(10):  # 6 dropped
                bus.publish(ev.DeviceLost(id="0001"))
            bus.close()
            return [e async for e in sub]

        received = asyncio.run(scenario())
        gap = received[0]
        assert gap.type == "stream.gap" and gap.seq == -1
        assert gap.data["dropped_count"] == 6
        assert [e.seq for e in received[1:]] == [7, 8, 9, 10]

    def test_publish_envelope_verbatim_for_replay(self) -> None:
        async def scenario() -> ev.Envelope:
            bus = ev.EventBus()
            sub = bus.subscribe()
            recorded = ev.Envelope(seq=99, ts=FIXED_TS, type="device.lost", data={"id": "0001"})
            bus.publish_envelope(recorded)
            bus.close()
            return await anext(aiter(sub))

        out = asyncio.run(scenario())
        assert out.seq == 99
        assert out.ts == FIXED_TS

    def test_publish_envelope_rejects_unknown_type(self) -> None:
        bus = ev.EventBus()
        bad = ev.Envelope(seq=1, ts=FIXED_TS, type="mystery.event", data={})
        with pytest.raises(ev.EventError):
            bus.publish_envelope(bad)

    def test_unsubscribe_ends_iteration(self) -> None:
        async def scenario() -> list[ev.Envelope]:
            bus = ev.EventBus()
            sub = bus.subscribe()
            bus.publish(ev.DeviceLost(id="0001"))
            bus.unsubscribe(sub)
            bus.publish(ev.DeviceLost(id="0002"))  # after unsubscribe: not delivered
            return [e async for e in sub]

        received = asyncio.run(scenario())
        assert [e.data["id"] for e in received] == ["0001"]

    def test_injectable_clock(self) -> None:
        bus = ev.EventBus(clock=lambda: FIXED_TS)
        env = bus.publish(ev.DeviceLost(id="0001"))
        assert env.ts == FIXED_TS
