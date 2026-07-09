"""Step 15: whole-daemon integration over fakes.

The daemon runs with a scripted scan backend and fake device links: a device
appears, is harvested end-to-end, files land in staging, the API serves the
story, the recorder writes it to disk, the single-instance lock holds, and a
mid-transfer shutdown retains the spool ``.part``.
"""

import asyncio
import json
from collections.abc import Callable
from pathlib import Path

import aiohttp
import pytest

from tempo_tb_ingest.config import Config
from tempo_tb_ingest.daemon import AlreadyRunning, Daemon
from tempo_tb_ingest.device.fake_link import FakeLink
from tempo_tb_ingest.recorder import load_recording
from tempo_tb_ingest.scanner import SMP_SERVICE_UUID, RawDetectionFn

FLIGHT = b'$PVER,"Tempo V2 1.5.0",114*72\r\n' + b"$PIMU,1,2,3*00\r\n" * 400

OWNERS = [{"deviceName": "Tempo-BT-0001", "jumperName": "riley", "isLoadOrganizer": True}]


def make_config(tmp_path: Path, port: int) -> Config:
    staging = tmp_path / "device-data"
    staging.mkdir(parents=True, exist_ok=True)
    (staging / "device-owners.json").write_text(json.dumps(OWNERS))
    return Config.model_validate(
        {
            "store": {
                "staging_root": str(staging),
                "data_dir": str(tmp_path / "data"),
            },
            "harvest": {"spool_dir": str(tmp_path / "data" / "spool")},
            "http": {"listen": f"127.0.0.1:{port}"},
            "detection": {"lost_after_s": 5, "absent_after_s": 10},
        }
    )


class PulsingBackend:
    """Scan backend emitting a Tempo advertisement every tick until stopped."""

    def __init__(self, mac: str = "AA:BB:CC:DD:EE:FF", name: str = "Tempo-BT-0001") -> None:
        self.mac = mac
        self.name = name
        self.paused = False

    async def __call__(
        self,
        detected: RawDetectionFn,
        stop: asyncio.Event,
        started: Callable[[], None],
    ) -> None:
        started()
        while not stop.is_set():
            if not self.paused:
                detected(self.mac, self.name, -60, [SMP_SERVICE_UUID])
            await asyncio.sleep(0.02)


async def wait_for(predicate: Callable[[], bool], timeout: float = 10.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.02)


@pytest.fixture
def port(unused_tcp_port_factory: Callable[[], int] | None = None) -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class TestDaemonStory:
    async def test_walk_in_the_door_end_to_end(self, tmp_path: Path, port: int) -> None:
        config = make_config(tmp_path, port)
        fake = FakeLink(sessions={"20260708/AABB1122": FLIGHT})
        daemon = Daemon(config, scan_backend=PulsingBackend(), link_factory=lambda a: fake)
        run_task = asyncio.create_task(daemon.run())
        try:
            staged = daemon.store.staging_path("0001", "20260708/AABB1122")
            await wait_for(staged.exists, timeout=15)
            assert staged.read_bytes() == FLIGHT

            async with (
                aiohttp.ClientSession() as http,
                http.get(f"http://127.0.0.1:{port}/state") as response,
            ):
                snapshot = await response.json()
            devices = {d["id"]: d for d in snapshot["devices"]}
            assert devices["0001"]["state"] == "PRESENT"
            assert devices["0001"]["sessions_known"] == 1
            assert devices["0001"]["jumper"] == "riley"
            assert snapshot["totals"]["sessions_stored"] == 1
            assert snapshot["totals"]["harvests_completed"] == 1
        finally:
            daemon.stop("test complete")
            await run_task

        # the recorder captured the whole story, ending with daemon.stopping
        recordings = list((config.store.data_dir / "events").glob("*.jsonl"))
        assert len(recordings) == 1
        envelopes, stats = load_recording(recordings[0])
        assert stats.skipped == 0
        types = [e.type for e in envelopes]
        assert types[0] == "daemon.started"
        assert types[-1] == "daemon.stopping"
        for expected in ("device.new", "device.returned", "harvest.completed"):
            assert expected in types

    async def test_single_instance_lock(self, tmp_path: Path, port: int) -> None:
        config = make_config(tmp_path, port)
        fake = FakeLink(sessions={})
        first = Daemon(config, scan_backend=PulsingBackend(), link_factory=lambda a: fake)
        run_task = asyncio.create_task(first.run())
        try:
            await wait_for(lambda: first._lock_file is not None, timeout=5)
            second_config = make_config(tmp_path, port + 1)
            second = Daemon(
                second_config, scan_backend=PulsingBackend(), link_factory=lambda a: fake
            )
            with pytest.raises(AlreadyRunning):
                await second.run()
        finally:
            first.stop()
            await run_task

        # lock released: a fresh instance may start now
        third = Daemon(
            make_config(tmp_path, port + 2),
            scan_backend=PulsingBackend(),
            link_factory=lambda a: fake,
        )
        third_task = asyncio.create_task(third.run())
        await wait_for(lambda: third._lock_file is not None, timeout=5)
        third.stop()
        await third_task

    async def test_shutdown_mid_transfer_keeps_part_file(self, tmp_path: Path, port: int) -> None:
        config = make_config(tmp_path, port)
        gate = asyncio.Event()
        progressed = asyncio.Event()

        async def slow_pause() -> None:
            # the pause hook fires on every fake operation; only block once a
            # download is actually in flight (spool open, transfer started)
            if any(c.startswith("download") for c in fake.call_log.calls):
                progressed.set()
                await gate.wait()  # hold the transfer mid-air until shutdown

        fake = FakeLink(sessions={"20260708/AABB1122": FLIGHT}, pause=slow_pause, chunk_size=64)
        daemon = Daemon(config, scan_backend=PulsingBackend(), link_factory=lambda a: fake)
        run_task = asyncio.create_task(daemon.run())
        await progressed.wait()  # a download is in flight and blocked
        daemon.stop("test: mid-transfer shutdown")
        gate.set()
        await run_task

        spool = daemon.store.spool_path("0001", "20260708/AABB1122")
        staged = daemon.store.staging_path("0001", "20260708/AABB1122")
        assert not staged.exists(), "aborted transfer must not be committed"
        # the spool file exists (possibly zero bytes yet); resume-ready
        assert spool.exists()

    async def test_healthz_and_ws_serve_live(self, tmp_path: Path, port: int) -> None:
        config = make_config(tmp_path, port)
        fake = FakeLink(sessions={})
        daemon = Daemon(config, scan_backend=PulsingBackend(), link_factory=lambda a: fake)
        run_task = asyncio.create_task(daemon.run())
        try:
            async with aiohttp.ClientSession() as http:
                await wait_for(lambda: daemon.bus.last_seq > 0, timeout=5)
                async with http.get(f"http://127.0.0.1:{port}/healthz") as response:
                    assert (await response.json()) == {"ok": True}
                async with http.ws_connect(f"http://127.0.0.1:{port}/events") as ws:
                    first = json.loads((await ws.receive()).data)
                    assert first["kind"] == "snapshot"
                    assert first["state"]["daemon"]["adapters"]["scan"] == "hci0"
        finally:
            daemon.stop()
            await run_task
