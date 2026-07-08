"""Step 11: harvest pipeline end-to-end over fakes, incl. fault scenarios."""

import asyncio
import hashlib
import itertools
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from tempo_tb_ingest.device.fake_link import FakeLink
from tempo_tb_ingest.device.protocol import log_path
from tempo_tb_ingest.events import Envelope, EventBus
from tempo_tb_ingest.harvest import HarvestWorker
from tempo_tb_ingest.owners import OwnersRegistry
from tempo_tb_ingest.store import Store

T0 = datetime(2026, 7, 8, 15, 0, 0, tzinfo=UTC)

FLIGHT_A = b'$PVER,"Tempo V2 1.5.0",114*72\r\n' + b"$PIMU,1,2,3*00\r\n" * 500
FLIGHT_B = b'$PVER,"Tempo V2 1.5.0",114*72\r\n' + b"$PENV,4,5*00\r\n" * 200

OWNERS = [
    {"deviceName": "Tempo-BT-0001", "jumperName": "riley", "isLoadOrganizer": True},
]


class Clock:
    def __init__(self) -> None:
        self.now = T0

    def __call__(self) -> datetime:
        return self.now

    def advance(self, s: float) -> None:
        self.now += timedelta(seconds=s)


class Harness:
    def __init__(self, tmp_path: Path, fake: FakeLink, device_id: str = "0001") -> None:
        self.bus = EventBus()
        self.subscription = self.bus.subscribe(queue_size=4096)
        self.clock = Clock()
        self.fake = fake
        owners_path = tmp_path / "device-owners.json"
        owners_path.write_text(json.dumps(OWNERS))
        self.store = Store(
            staging_root=tmp_path / "device-data",
            data_dir=tmp_path / "data",
            spool_dir=tmp_path / "data" / "spool",
            bus=self.bus,
        )
        self.harvested: list[str] = []
        self.worker = HarvestWorker(
            self.bus,
            self.store,
            OwnersRegistry(owners_path, self.bus),
            link_factory=lambda address: fake,
            resolve_target=lambda d: (f"Tempo-BT-{d}", "AA:BB:CC:DD:EE:FF"),
            max_attempts=3,
            retry_cooldown_s=15.0,
            progress_interval_s=0.0,
            clock=self.clock,
            on_harvested=self.harvested.append,
        )

    def run(self) -> None:
        asyncio.run(self.worker.run_until_idle())

    def events(self) -> list[Envelope]:
        self.bus.close()

        async def drain() -> list[Envelope]:
            return [e async for e in self.subscription]

        return asyncio.run(drain())

    def types(self) -> list[str]:
        return [e.type for e in self.events() if not e.type.startswith("owners.reloaded")]


def make_fake(**kwargs: object) -> FakeLink:
    defaults: dict[str, object] = {
        "sessions": {"20260705/1CDD8C18": FLIGHT_A, "20260705/00BAF6AB": FLIGHT_B}
    }
    defaults.update(kwargs)
    return FakeLink(**defaults)  # type: ignore[arg-type]


class TestHappyPath:
    def test_full_walk_in_the_door_story(self, tmp_path: Path) -> None:
        h = Harness(tmp_path, make_fake())
        h.worker.request("0001")
        h.run()

        types = h.types()
        # progress cadence is chunk-count-dependent; assert the story shape
        # with progress collapsed, then that each transfer did report progress
        collapsed = [t for t, _ in itertools.groupby(types)]
        assert collapsed == [
            "harvest.queued",
            "harvest.started",
            "harvest.session_list",
            "transfer.started",
            "transfer.progress",
            "transfer.completed",
            "store.session_added",
            "transfer.started",
            "transfer.progress",
            "transfer.completed",
            "store.session_added",
            "harvest.completed",
        ]
        staged_a = h.store.staging_path("0001", "20260705/1CDD8C18")
        staged_b = h.store.staging_path("0001", "20260705/00BAF6AB")
        assert staged_a.read_bytes() == FLIGHT_A
        assert staged_b.read_bytes() == FLIGHT_B
        assert h.harvested == ["0001"]
        # harvest-time attribution recorded
        sessions = h.store.sessions("0001")
        assert all(s.jumper == "riley" and s.jumper_is_lo for s in sessions)

    def test_event_payloads_coherent(self, tmp_path: Path) -> None:
        h = Harness(tmp_path, make_fake())
        h.worker.request("0001")
        h.run()
        events = h.events()
        by_type: dict[str, list[Envelope]] = {}
        for e in events:
            by_type.setdefault(e.type, []).append(e)
        listing = by_type["harvest.session_list"][0].data
        assert listing == {"id": "0001", "count": 2, "new_count": 2, "truncated": False}
        completed = by_type["harvest.completed"][0].data
        assert completed["sessions_downloaded"] == 2
        assert completed["bytes"] == len(FLIGHT_A) + len(FLIGHT_B)
        # session keys are processed in listing order (sorted): 00BAF6AB first
        added = {e.data["session_key"]: e.data for e in by_type["store.session_added"]}
        assert added["20260705/1CDD8C18"]["sha256"] == hashlib.sha256(FLIGHT_A).hexdigest()
        assert added["20260705/00BAF6AB"]["sha256"] == hashlib.sha256(FLIGHT_B).hexdigest()
        assert all(d["jumper"] == "riley" for d in added.values())

    def test_never_writes_to_device(self, tmp_path: Path) -> None:
        h = Harness(tmp_path, make_fake())
        h.worker.request("0001")
        h.run()
        forbidden = ("write", "upload", "delete", "logger", "settings_set", "led")
        for call in h.fake.call_log.calls:
            assert not any(w in call.lower() for w in forbidden), call

    def test_nothing_new_completes_quietly(self, tmp_path: Path) -> None:
        h = Harness(tmp_path, make_fake())
        h.worker.request("0001")
        h.run()
        h.worker.request("0001")
        h.run()
        completed = [e for e in h.events() if e.type == "harvest.completed"]
        assert completed[1].data["sessions_downloaded"] == 0

    def test_coalescing(self, tmp_path: Path) -> None:
        h = Harness(tmp_path, make_fake())
        h.worker.request("0001")
        h.worker.request("0001")
        h.run()
        assert h.types().count("harvest.queued") == 1


class TestAttribution:
    def test_unmapped_device_stored_null_and_surfaced(self, tmp_path: Path) -> None:
        h = Harness(tmp_path, make_fake())
        h.worker._resolve_target = lambda d: (f"Tempo-BT-{d}", None)
        h.worker.request("0004")  # not in the registry
        h.run()
        types = h.types()
        assert "owners.unmapped" in types
        sessions = h.store.sessions("0004")
        assert len(sessions) == 2
        assert all(s.jumper is None for s in sessions)

    def test_truncated_listing_surfaced(self, tmp_path: Path) -> None:
        h = Harness(tmp_path, make_fake(truncated=True))
        h.worker.request("0001")
        h.run()
        assert "harvest.truncated" in h.types()


class TestFailureAndRetry:
    def test_connect_failure_then_sighting_retry(self, tmp_path: Path) -> None:
        h = Harness(tmp_path, make_fake(connect_failures=1))
        h.worker.request("0001")
        h.run()
        # cooldown not yet passed: sighting does nothing
        h.worker.notify_sighting("0001")
        h.run()
        # cooldown passed: sighting re-queues attempt 2
        h.clock.advance(20)
        h.worker.notify_sighting("0001")
        h.run()

        events = h.events()
        failed = [e for e in events if e.type == "harvest.failed"]
        assert len(failed) == 1
        assert failed[0].data["will_retry"] is True
        assert "connect" in failed[0].data["reason"]
        queued = [e.data["attempt"] for e in events if e.type == "harvest.queued"]
        assert queued == [1, 2]
        assert [e.type for e in events][-1] == "harvest.completed"

    def test_drop_mid_transfer_resumes_byte_identical(self, tmp_path: Path) -> None:
        key = "20260705/1CDD8C18"
        h = Harness(tmp_path, make_fake(drop_at={log_path(key): 2048}))
        h.worker.request("0001")
        h.run()

        spool = h.store.spool_path("0001", key)
        assert spool.stat().st_size == 2048  # partial retained

        h.clock.advance(20)
        h.worker.notify_sighting("0001")
        h.run()

        assert not spool.exists()  # consumed by commit
        staged = h.store.staging_path("0001", key)
        assert hashlib.sha256(staged.read_bytes()).hexdigest() == (
            hashlib.sha256(FLIGHT_A).hexdigest()
        )
        assert any("offset=2048" in c for c in h.fake.call_log.calls)  # true resume
        resumed = [
            e for e in h.events() if e.type == "transfer.started" and e.data["resumed_from"] > 0
        ]
        assert len(resumed) == 1

    def test_max_attempts_exhaustion_is_loud_and_final(self, tmp_path: Path) -> None:
        h = Harness(tmp_path, make_fake(connect_failures=99))
        h.worker.request("0001")
        h.run()
        for _ in range(4):
            h.clock.advance(20)
            h.worker.notify_sighting("0001")
            h.run()

        failed = [e.data for e in h.events() if e.type == "harvest.failed"]
        assert len(failed) == 3  # max_attempts
        assert [f["will_retry"] for f in failed] == [True, True, False]
        # further sightings do nothing
        h.worker.notify_sighting("0001")
        assert h.worker._queued == set()

    def test_new_request_resets_attempt_counter(self, tmp_path: Path) -> None:
        h = Harness(tmp_path, make_fake(connect_failures=1))
        h.worker.request("0001")
        h.run()  # attempt 1 fails
        h.worker.request("0001")  # a fresh RETURNED trigger
        h.run()
        queued = [e.data["attempt"] for e in h.events() if e.type == "harvest.queued"]
        assert queued == [1, 1]

    def test_empty_file_skipped_loudly_not_stored(self, tmp_path: Path) -> None:
        h = Harness(tmp_path, make_fake(sessions={"20260705/EEEEEEEE": b""}))
        h.worker.request("0001")
        h.run()
        types = h.types()
        assert "store.error" in types
        assert "store.session_added" not in types
        assert h.store.sessions("0001") == []
        assert not h.store.spool_path("0001", "20260705/EEEEEEEE").exists()
        # the job itself completes (with zero downloads), not fails
        assert types[-1] == "harvest.completed"

    def test_bad_session_does_not_block_good_ones(self, tmp_path: Path) -> None:
        """Real-data case (0001's card): a zero-byte 19700101 boot artifact
        must not prevent harvesting everything else."""
        h = Harness(
            tmp_path,
            make_fake(
                sessions={
                    "19700101/C4F3BC90": b"",  # listed first
                    "20260705/1CDD8C18": FLIGHT_A,
                }
            ),
        )
        h.worker.request("0001")
        h.run()
        events = h.events()
        types = [e.type for e in events]
        assert "store.error" in types
        assert types[-1] == "harvest.completed"
        completed = [e for e in events if e.type == "harvest.completed"]
        assert completed[0].data["sessions_downloaded"] == 1
        staged = h.store.staging_path("0001", "20260705/1CDD8C18")
        assert staged.read_bytes() == FLIGHT_A
        assert h.store.known_sessions("0001") == {"20260705/1CDD8C18"}
