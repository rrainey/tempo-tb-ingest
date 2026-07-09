"""Step 8: scanner — filtering, sighting mapping, backoff/recovery, live smoke."""

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest

from tempo_tb_ingest import events as ev
from tempo_tb_ingest.scanner import (
    SMP_SERVICE_UUID,
    RawDetectionFn,
    Scanner,
    Sighting,
    bleak_backend,
    is_tempo_advertisement,
)

T0 = datetime(2026, 7, 8, 14, 0, 0, tzinfo=UTC)


class TestFiltering:
    def test_smp_uuid_passes_even_unnamed(self) -> None:
        assert is_tempo_advertisement(None, [SMP_SERVICE_UUID])

    def test_uuid_match_case_insensitive(self) -> None:
        assert is_tempo_advertisement(None, [SMP_SERVICE_UUID.upper()])

    def test_tempo_name_passes_without_uuid(self) -> None:
        assert is_tempo_advertisement("Tempo-BT-0001", [])
        assert is_tempo_advertisement("Tempo-BT", [])  # unprovisioned

    def test_others_rejected(self) -> None:
        battery_service = "0000180f-0000-1000-8000-00805f9b34fb"
        assert not is_tempo_advertisement("Fitbit Charge", [battery_service])
        assert not is_tempo_advertisement(None, [])


class ScriptedBackend:
    """Deterministic backend: runs scripted actions per session.

    ``on_exhausted`` (wired to ``scanner.stop`` by the harness) fires when a
    session's script runs dry — the backend itself can only end its own
    session, not the scanner (mirrors the real pause/stop split)."""

    def __init__(self) -> None:
        self.sessions = 0
        self.script: list[Callable[[RawDetectionFn], None] | Exception] = []
        self.on_session: Callable[[int], list[Callable[[RawDetectionFn], None] | Exception]]
        self.on_exhausted: Callable[[], None] = lambda: None

    async def __call__(
        self,
        detected: RawDetectionFn,
        stop: asyncio.Event,
        started: Callable[[], None],
    ) -> None:
        actions = self.on_session(self.sessions)
        self.sessions += 1
        if actions and isinstance(actions[0], Exception):
            raise actions[0]  # start failure: scanning never began
        started()
        for action in actions:
            if isinstance(action, Exception):
                raise action  # mid-scan failure
            action(detected)
            await asyncio.sleep(0)
        if actions:
            self.on_exhausted()  # script dry: harness ends the scanner
        await stop.wait()  # a real backend runs until its session is stopped


def tempo_detection(
    mac: str, name: str | None, rssi: int = -60
) -> Callable[[RawDetectionFn], None]:
    return lambda detected: detected(mac, name, rssi, [SMP_SERVICE_UUID])


def noise_detection() -> Callable[[RawDetectionFn], None]:
    return lambda detected: detected("11:22:33:44:55:66", "SomeWatch", -50, [])


async def collect(scanner: Scanner) -> list[Sighting]:
    return [s async for s in scanner.sightings()]


class TestScanner:
    def test_sightings_filtered_and_mapped(self) -> None:
        backend = ScriptedBackend()
        backend.on_session = lambda n: [
            tempo_detection("DC:BD:F1:0D:F1:D9", "Tempo-BT-0001", rssi=-58),
            noise_detection(),
            tempo_detection("AA:BB:CC:DD:EE:FF", None, rssi=-80),
        ]

        async def scenario() -> list[Sighting]:
            bus = ev.EventBus()
            scanner = Scanner(bus, backend, clock=lambda: T0)
            backend.on_exhausted = scanner.stop
            run = asyncio.create_task(scanner.run())
            got = await collect(scanner)
            await run
            return got

        got = asyncio.run(scenario())
        assert got == [
            Sighting(mac="DC:BD:F1:0D:F1:D9", name="Tempo-BT-0001", rssi=-58, ts=T0),
            Sighting(mac="AA:BB:CC:DD:EE:FF", name=None, rssi=-80, ts=T0),
        ]

    def test_backend_failure_degrades_backs_off_recovers(self) -> None:
        backend = ScriptedBackend()
        boom = RuntimeError("org.bluez.Error.NotReady")
        backend.on_session = lambda n: (
            [boom] if n < 3 else [tempo_detection("DC:BD:F1:0D:F1:D9", "Tempo-BT-0001")]
        )
        sleeps: list[float] = []
        now = {"t": T0}

        def clock() -> datetime:
            return now["t"]

        async def fake_sleep(s: float) -> None:
            sleeps.append(s)
            now["t"] += timedelta(seconds=s)

        async def scenario() -> tuple[list[ev.Envelope], list[Sighting]]:
            bus = ev.EventBus()
            sub = bus.subscribe()
            scanner = Scanner(bus, backend, clock=clock, sleep=fake_sleep)
            backend.on_exhausted = scanner.stop
            run = asyncio.create_task(scanner.run())
            got = await collect(scanner)
            await run
            bus.close()
            return [e async for e in sub], got

        published, got = asyncio.run(scenario())
        degraded = [e for e in published if e.type == "scanner.degraded"]
        recovered = [e for e in published if e.type == "scanner.recovered"]
        assert len(degraded) == 3
        assert "NotReady" in degraded[0].data["reason"]
        assert sleeps == [1.0, 2.0, 4.0]  # exponential backoff
        assert len(recovered) == 1
        assert recovered[0].data["outage_s"] == pytest.approx(7.0)  # sum of backoffs
        assert len(got) == 1  # scanning resumed and delivered

    def test_backoff_caps(self) -> None:
        backend = ScriptedBackend()
        failures = 8
        backend.on_session = lambda n: (
            [RuntimeError("x")] if n < failures else [tempo_detection("AA", "Tempo-BT-0001")]
        )
        sleeps: list[float] = []

        async def fake_sleep(s: float) -> None:
            sleeps.append(s)

        async def scenario() -> None:
            bus = ev.EventBus()
            scanner = Scanner(bus, backend, sleep=fake_sleep, backoff_max_s=10.0)
            backend.on_exhausted = scanner.stop
            run = asyncio.create_task(scanner.run())
            await collect(scanner)
            await run

        asyncio.run(scenario())
        assert sleeps == [1.0, 2.0, 4.0, 8.0, 10.0, 10.0, 10.0, 10.0]

    def test_overflow_sheds_oldest(self) -> None:
        backend = ScriptedBackend()
        backend.on_session = lambda n: [
            tempo_detection("AA", "Tempo-BT-0001", rssi=-i) for i in range(10)
        ]

        async def scenario() -> tuple[list[Sighting], int]:
            bus = ev.EventBus()
            scanner = Scanner(bus, backend, queue_size=4)
            backend.on_exhausted = scanner.stop
            run = asyncio.create_task(scanner.run())
            # don't consume until the producer finished: forces overflow
            await run
            got = await collect(scanner)
            return got, scanner.dropped

        got, dropped = asyncio.run(scenario())
        assert dropped == 6
        assert [s.rssi for s in got] == [-6, -7, -8, -9]  # newest kept


class ContinuousBackend:
    """Emits one Tempo advertisement per tick until its session is stopped."""

    def __init__(self) -> None:
        self.sessions = 0
        self.active = False

    async def __call__(
        self,
        detected: RawDetectionFn,
        stop: asyncio.Event,
        started: Callable[[], None],
    ) -> None:
        self.sessions += 1
        self.active = True
        started()
        try:
            while not stop.is_set():
                detected("AA:BB:CC:DD:EE:FF", "Tempo-BT-0001", -60, [SMP_SERVICE_UUID])
                await asyncio.sleep(0.001)
        finally:
            self.active = False


class TestPauseResume:
    """The daemon pauses scanning during connections (BlueZ InProgress)."""

    def test_pause_ends_session_resume_starts_new_one(self) -> None:
        async def scenario() -> tuple[int, list[str]]:
            bus = ev.EventBus()
            sub = bus.subscribe()
            backend = ContinuousBackend()
            scanner = Scanner(bus, backend)
            run = asyncio.create_task(scanner.run())
            await asyncio.sleep(0.02)
            assert backend.active

            await scanner.pause()
            assert not backend.active  # pause() returns only once truly idle
            assert backend.sessions == 1
            await asyncio.sleep(0.02)
            assert backend.sessions == 1  # stays down while paused

            scanner.resume()
            await asyncio.sleep(0.02)
            assert backend.active
            assert backend.sessions == 2  # a fresh session after resume

            scanner.stop()
            await run
            bus.close()
            return backend.sessions, [e.type async for e in sub]

        sessions, types = asyncio.run(scenario())
        assert sessions == 2
        # a pause is not an outage: no degraded/recovered noise
        assert "scanner.degraded" not in types
        assert "scanner.recovered" not in types

    def test_stop_while_paused_exits(self) -> None:
        async def scenario() -> None:
            bus = ev.EventBus()
            backend = ContinuousBackend()
            scanner = Scanner(bus, backend)
            run = asyncio.create_task(scanner.run())
            await asyncio.sleep(0.02)
            await scanner.pause()
            scanner.stop()
            await asyncio.wait_for(run, timeout=2)

        asyncio.run(scenario())


@pytest.mark.live
class TestLiveSmoke:
    async def test_real_scan_sees_a_tempo_device(self) -> None:
        bus = ev.EventBus()
        scanner = Scanner(bus, bleak_backend())
        run = asyncio.create_task(scanner.run())
        found: list[Sighting] = []

        async def consume() -> None:
            async for s in scanner.sightings():
                found.append(s)
                if s.name and s.name.startswith("Tempo-BT"):
                    return

        try:
            await asyncio.wait_for(consume(), timeout=30)
        finally:
            scanner.stop()
            await run
        named = [s for s in found if s.name and s.name.startswith("Tempo-BT")]
        assert named, f"no Tempo-BT sighting in 30 s (saw {len(found)} filtered adverts)"
        assert all(s.rssi < 0 for s in named)
