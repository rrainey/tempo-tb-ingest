"""Step 14: API contract — /state, snapshot-first WS, gap marker, static.

The /state wire format is locked by a golden fixture rendered from the
synthetic-day recording (regenerate deliberately with REGEN_FIXTURES=1).
"""

import asyncio
import json
import os
from pathlib import Path
from typing import Any

from aiohttp.test_utils import TestClient, TestServer

from tempo_tb_ingest import events as ev
from tempo_tb_ingest.api import create_app
from tempo_tb_ingest.recorder import load_recording
from tempo_tb_ingest.statefold import StateFold

FIXTURES = Path(__file__).parent / "fixtures"
SYNTHETIC = FIXTURES / "synthetic-day.jsonl"
GOLDEN_STATE = FIXTURES / "state-snapshot.json"


def folded_synthetic() -> tuple[ev.EventBus, StateFold]:
    """A bus + fold with the whole synthetic day already applied."""
    bus = ev.EventBus()
    fold = StateFold(version="0.1.0", adapters={"scan": "hci0", "transfer": ["hci0"]})
    envelopes, stats = load_recording(SYNTHETIC)
    assert stats.skipped == 0
    for env in envelopes:
        bus.publish_envelope(env)
        fold.apply(env)
    return bus, fold


async def client_for(bus: ev.EventBus, fold: StateFold) -> TestClient:  # type: ignore[type-arg]
    app = create_app(bus, fold.snapshot)
    client = TestClient(TestServer(app))
    await client.start_server()
    return client


class TestHttp:
    async def test_healthz(self) -> None:
        bus, fold = folded_synthetic()
        client = await client_for(bus, fold)
        try:
            response = await client.get("/healthz")
            assert response.status == 200
            assert await response.json() == {"ok": True}
        finally:
            await client.close()

    async def test_state_matches_golden_fixture(self) -> None:
        _, fold = folded_synthetic()
        snapshot = fold.snapshot()
        rendered = json.dumps(snapshot, indent=2, sort_keys=True) + "\n"
        if os.environ.get("REGEN_FIXTURES"):
            GOLDEN_STATE.write_text(rendered)
        assert GOLDEN_STATE.is_file(), "missing golden state fixture; see module docstring"
        assert rendered == GOLDEN_STATE.read_text(), (
            "snapshot wire format changed; if intentional, regenerate fixtures"
        )

    async def test_state_semantic_expectations(self) -> None:
        _, fold = folded_synthetic()
        snapshot = fold.snapshot()
        assert snapshot["totals"] == {
            "sessions_stored": 3,
            "bytes_stored": 2922782 + 2875691 + 3100000,
            "pending_download": 0,  # everything discovered was downloaded
            "harvests_completed": 2,
            "failures": 1,
        }
        devices = {d["id"]: d for d in snapshot["devices"]}
        assert devices["0001"]["sessions_known"] == 3
        assert devices["0001"]["pending_download"] == 0
        assert devices["0001"]["jumper"] == "riley"
        assert devices["0001"]["state"] == "PRESENT"
        assert snapshot["active_job"] is None
        assert snapshot["queue"] == []
        assert snapshot["daemon"]["scanning"] is False  # daemon.stopping was replayed

    async def test_pending_download_lingers_after_failure(self) -> None:
        """Discovered-but-not-downloaded persists across a failed harvest —
        exactly when the stat matters (dashboard-notes semantics)."""
        fold = StateFold()
        bus = ev.EventBus()
        for data in (
            ev.HarvestStarted(id="0001", attempt=1),
            ev.HarvestSessionList(id="0001", count=5, new_count=2, truncated=False),
            ev.StoreSessionAdded(
                id="0001",
                session_key="20260709/AAAAAAAA",
                path="x",
                size=10,
                sha256="0" * 64,
                jumper=None,
            ),
            ev.HarvestFailed(id="0001", reason="disconnected", attempt=1, will_retry=True),
        ):
            fold.apply(bus.publish(data))
        snapshot = fold.snapshot()
        devices = {d["id"]: d for d in snapshot["devices"]}
        assert devices["0001"]["pending_download"] == 1  # one still on the device
        assert snapshot["totals"]["pending_download"] == 1

    async def test_daemon_started_resets_fold(self) -> None:
        """Loop-replay boundary (or a real daemon restart) must not
        double-count — the fold observed at 67k sessions after a night of
        looping (2026-07-10)."""
        fold = StateFold()
        bus = ev.EventBus()
        fold.apply(
            bus.publish(
                ev.StoreSessionAdded(
                    id="0001",
                    session_key="20260709/AAAAAAAA",
                    path="x",
                    size=10,
                    sha256="0" * 64,
                    jumper=None,
                )
            )
        )
        assert fold.snapshot()["totals"]["sessions_stored"] == 1
        fold.apply(bus.publish(ev.DaemonStarted(version="replay-loop", config={})))
        snapshot = fold.snapshot()
        assert snapshot["totals"]["sessions_stored"] == 0
        assert snapshot["devices"] == []

    async def test_static_serving(self, tmp_path: Path) -> None:
        (tmp_path / "index.html").write_text("<title>tempo</title>")
        bus, fold = folded_synthetic()
        app = create_app(bus, fold.snapshot, static_dir=tmp_path)
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            response = await client.get("/")
            assert response.status == 200
            assert "tempo" in await response.text()
        finally:
            await client.close()


class TestWebSocket:
    async def test_snapshot_first_then_coherent_stream(self) -> None:
        bus, fold = folded_synthetic()
        client = await client_for(bus, fold)
        try:
            ws = await client.ws_connect("/events")
            first = json.loads((await ws.receive()).data)
            assert first["kind"] == "snapshot"
            snapshot_seq = first["state"]["seq"]
            assert snapshot_seq == 34  # the whole synthetic day

            published = [bus.publish(ev.DeviceLost(id="0001")) for _ in range(3)]
            received: list[dict[str, Any]] = []
            for _ in published:
                frame = json.loads((await ws.receive()).data)
                assert frame["kind"] == "event"
                received.append(frame["event"])
            seqs = [e["seq"] for e in received]
            assert seqs == [snapshot_seq + 1, snapshot_seq + 2, snapshot_seq + 3]
            await ws.close()
        finally:
            await client.close()

    async def test_events_already_in_snapshot_are_filtered(self) -> None:
        """A publish landing between WS-subscribe and snapshot-build must not
        be delivered twice: the seq filter drops it."""
        bus, fold = folded_synthetic()

        real_snapshot = fold.snapshot

        def racy_snapshot() -> dict[str, Any]:
            env = bus.publish(ev.DeviceLost(id="0002"))  # lands in the WS queue
            fold.apply(env)  # …and in the snapshot
            return real_snapshot()

        app = create_app(bus, racy_snapshot)
        client = TestClient(TestServer(app))
        await client.start_server()
        try:
            ws = await client.ws_connect("/events")
            first = json.loads((await ws.receive()).data)
            snapshot_seq = first["state"]["seq"]
            assert snapshot_seq == 35  # includes the racy publish

            follow_up = bus.publish(ev.DeviceLost(id="0003"))
            frame = json.loads((await ws.receive()).data)
            assert frame["event"]["seq"] == follow_up.seq  # 0002 was filtered
            assert frame["event"]["data"]["id"] == "0003"
            await ws.close()
        finally:
            await client.close()

    async def test_slow_client_receives_gap_marker(self) -> None:
        bus = ev.EventBus(queue_size=4)
        fold = StateFold()
        client = await client_for(bus, fold)
        try:
            ws = await client.ws_connect("/events")
            first = json.loads((await ws.receive()).data)
            assert first["kind"] == "snapshot"

            # overwhelm the subscriber queue before the pump can drain it
            for _ in range(50):
                bus.publish(ev.DeviceLost(id="0001"))
            await asyncio.sleep(0)

            saw_gap = False
            for _ in range(50):
                msg = await asyncio.wait_for(ws.receive(), timeout=5)
                frame = json.loads(msg.data)
                if frame["kind"] == "event" and frame["event"]["type"] == "stream.gap":
                    saw_gap = True
                    assert frame["event"]["data"]["dropped_count"] > 0
                    break
            assert saw_gap
            await ws.close()
        finally:
            await client.close()
