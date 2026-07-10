"""Step 4: recorder (JSONL, daily rotation) and replay (pacing, loudness)."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tempo_tb_ingest import events as ev
from tempo_tb_ingest.recorder import Recorder, load_recording, replay

FIXTURE = Path(__file__).parent / "fixtures" / "synthetic-day.jsonl"

T0 = datetime(2026, 7, 8, 14, 0, 0, tzinfo=UTC)


class TickingClock:
    def __init__(self, start: datetime = T0) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def publish_some(bus: ev.EventBus, clock: TickingClock, n: int = 5) -> list[ev.Envelope]:
    out = []
    for i in range(n):
        seen = ev.DeviceSeen(id="0001", mac="AA", name="Tempo-BT-0001", rssi=-60 - i)
        out.append(bus.publish(seen))
        clock.advance(10)
    return out


class TestRecorder:
    def test_round_trip_byte_identical(self, tmp_path: Path) -> None:
        clock = TickingClock()
        bus = ev.EventBus(clock=clock)
        recorder = Recorder(bus, tmp_path)
        published = publish_some(bus, clock)

        async def drain() -> None:
            bus.close()
            await recorder.run()

        asyncio.run(drain())

        recording = tmp_path / "20260708.jsonl"
        assert recording.read_text() == "".join(e.to_json() + "\n" for e in published)

        # ...and replaying reproduces the very same envelopes
        replay_bus = ev.EventBus()
        collected: list[ev.Envelope] = []
        sub = replay_bus.subscribe()

        async def run_replay() -> None:
            async def collect() -> None:
                async for e in sub:
                    collected.append(e)

            task = asyncio.create_task(collect())
            await replay(recording, replay_bus, speed=1e9)
            replay_bus.close()
            await task

        asyncio.run(run_replay())
        assert [e.to_json() for e in collected] == [e.to_json() for e in published]

    def test_daily_rotation_by_event_timestamp(self, tmp_path: Path) -> None:
        clock = TickingClock()
        bus = ev.EventBus(clock=clock)
        recorder = Recorder(bus, tmp_path)
        env_day1 = bus.publish(ev.DeviceLost(id="0001"))
        clock.advance(24 * 3600)
        env_day2 = bus.publish(ev.DeviceLost(id="0002"))
        recorder.write(env_day1)
        recorder.write(env_day2)
        recorder.close()
        assert (tmp_path / "20260708.jsonl").is_file()
        assert (tmp_path / "20260709.jsonl").is_file()

    def test_append_not_truncate(self, tmp_path: Path) -> None:
        clock = TickingClock()
        bus = ev.EventBus(clock=clock)
        first = Recorder(bus, tmp_path)
        first.write(bus.publish(ev.DeviceLost(id="0001")))
        first.close()
        second = Recorder(bus, tmp_path)  # daemon restart, same day
        second.write(bus.publish(ev.DeviceLost(id="0002")))
        second.close()
        lines = (tmp_path / "20260708.jsonl").read_text().splitlines()
        assert len(lines) == 2


class TestReplay:
    def test_malformed_lines_skipped_loudly(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        good = ev.Envelope(seq=1, ts=T0, type="device.lost", data={"id": "0001"}).to_json()
        recording = tmp_path / "r.jsonl"
        recording.write_text(f"{good}\nthis is not json\n{good}\n")
        with caplog.at_level(logging.WARNING):
            envelopes, stats = load_recording(recording)
        assert len(envelopes) == 2
        assert stats.skipped == 1
        assert stats.skipped_lines == [2]
        assert "skipping malformed event line" in caplog.text

    def test_pacing_honors_speed(self, tmp_path: Path) -> None:
        e1 = ev.Envelope(seq=1, ts=T0, type="device.lost", data={"id": "0001"})
        t2 = T0 + timedelta(seconds=10)
        e2 = ev.Envelope(seq=2, ts=t2, type="device.lost", data={"id": "0002"})
        recording = tmp_path / "r.jsonl"
        recording.write_text(e1.to_json() + "\n" + e2.to_json() + "\n")

        sleeps: list[float] = []

        async def fake_sleep(s: float) -> None:
            sleeps.append(s)

        asyncio.run(replay(recording, ev.EventBus(), speed=4.0, sleep=fake_sleep))
        assert sleeps == [2.5]  # 10 s gap at 4x

    def test_loop_cycles(self, tmp_path: Path) -> None:
        e1 = ev.Envelope(seq=1, ts=T0, type="device.lost", data={"id": "0001"})
        recording = tmp_path / "r.jsonl"
        recording.write_text(e1.to_json() + "\n")
        stats = asyncio.run(replay(recording, ev.EventBus(), loop=True, max_cycles=3, speed=1e9))
        assert stats.cycles == 3
        assert stats.published == 3

    def test_loop_reseqences_and_marks_restarts(self, tmp_path: Path) -> None:
        """Looping must not repeat seq numbers (clients drop stale seqs) and
        must publish a daemon.started boundary so folds reset — the frozen
        looping-demo bug, 2026-07-10."""
        e1 = ev.Envelope(seq=5, ts=T0, type="device.lost", data={"id": "0001"})
        recording = tmp_path / "r.jsonl"
        recording.write_text(e1.to_json() + "\n")

        async def scenario() -> list[ev.Envelope]:
            bus = ev.EventBus()
            sub = bus.subscribe(queue_size=64)
            await replay(recording, bus, loop=True, max_cycles=3, speed=1e9)
            bus.close()
            return [e async for e in sub]

        received = asyncio.run(scenario())
        types = [e.type for e in received]
        assert types == [
            "device.lost",
            "daemon.started",
            "device.lost",
            "daemon.started",
            "device.lost",
        ]
        seqs = [e.seq for e in received]
        assert seqs == sorted(seqs)
        assert len(set(seqs)) == len(seqs)  # strictly increasing, no repeats

    def test_empty_recording_is_an_error(self, tmp_path: Path) -> None:
        recording = tmp_path / "r.jsonl"
        recording.write_text("garbage\n")
        with pytest.raises(ev.EventError, match="no valid events"):
            asyncio.run(replay(recording, ev.EventBus()))

    def test_bad_speed_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="speed"):
            asyncio.run(replay(tmp_path / "r.jsonl", ev.EventBus(), speed=0))


class TestSyntheticDayFixture:
    """The checked-in story fixture: standing input for API/dashboard tests."""

    def test_loads_cleanly(self) -> None:
        envelopes, stats = load_recording(FIXTURE)
        assert stats.skipped == 0
        assert len(envelopes) == 34
        assert envelopes[0].type == "daemon.started"
        assert envelopes[-1].type == "daemon.stopping"
        seqs = [e.seq for e in envelopes]
        assert seqs == sorted(seqs)

    def test_tells_the_harvest_story(self) -> None:
        envelopes, _ = load_recording(FIXTURE)
        types = [e.type for e in envelopes]
        # a resumable failure followed by a successful resumed transfer
        assert "transfer.failed" in types
        resumed = [
            e for e in envelopes if e.type == "transfer.started" and e.data["resumed_from"] > 0
        ]
        assert len(resumed) == 1
        assert types.count("harvest.completed") == 2
