"""HTTP/WS API: GET /state, WS /events, GET /healthz, static dashboard.

Wire contract per design §6, with one strengthening over the original text:
the WebSocket's **first frame is a snapshot**, taken after the subscription
begins, so a client can never miss events between snapshot and stream — the
snapshot-then-stream race is structurally impossible. ``GET /state`` remains
for polling and diagnostics.

WS frames:
    {"kind": "snapshot", "state": {…§6.1…}}     first frame, and only then
    {"kind": "event", "event": {…§6.2 envelope…}}
A ``stream.gap`` event (per-subscriber, seq = -1) means this client was too
slow and must treat the next snapshot-bearing reconnect as authoritative.
"""

import asyncio
import contextlib
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from aiohttp import WSMsgType, web

from tempo_tb_ingest.events import EventBus

#: returns the §6.1 snapshot structure; "seq" must be the bus seq of the last
#: event already reflected in the snapshot (coherence anchor for clients)
SnapshotFn = Callable[[], dict[str, Any]]

_APP_KEY_BUS = web.AppKey("bus", EventBus)
_APP_KEY_SNAPSHOT = web.AppKey("snapshot_fn", object)


def create_app(
    bus: EventBus,
    snapshot_fn: SnapshotFn,
    *,
    static_dir: Path | None = None,
) -> web.Application:
    app = web.Application()
    app[_APP_KEY_BUS] = bus
    app[_APP_KEY_SNAPSHOT] = snapshot_fn
    app.router.add_get("/healthz", _healthz)
    app.router.add_get("/state", _state)
    app.router.add_get("/events", _events_ws)
    if static_dir is not None and static_dir.is_dir():
        app.router.add_get("/", _index_factory(static_dir))
        app.router.add_static("/", static_dir)
    return app


async def _healthz(request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def _state(request: web.Request) -> web.Response:
    snapshot_fn = request.app[_APP_KEY_SNAPSHOT]
    return web.json_response(snapshot_fn())  # type: ignore[operator]


async def _events_ws(request: web.Request) -> web.WebSocketResponse:
    bus = request.app[_APP_KEY_BUS]
    snapshot_fn = request.app[_APP_KEY_SNAPSHOT]
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)

    # subscribe FIRST, snapshot second: anything published in between is
    # queued in the subscription and simply filtered by seq below
    subscription = bus.subscribe()
    try:
        snapshot = snapshot_fn()  # type: ignore[operator]
        snapshot_seq = int(snapshot.get("seq", 0))
        await ws.send_str(json.dumps({"kind": "snapshot", "state": snapshot}))

        async def pump() -> None:
            async for env in subscription:
                if 0 < env.seq <= snapshot_seq:
                    continue  # already reflected in the snapshot
                frame = {"kind": "event", "event": json.loads(env.to_json())}
                await ws.send_str(json.dumps(frame))

        pump_task = asyncio.create_task(pump())
        try:
            # drain (and ignore) client frames; exit on close
            async for msg in ws:
                if msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.ERROR):
                    break
        finally:
            pump_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pump_task
    finally:
        bus.unsubscribe(subscription)
        with contextlib.suppress(Exception):
            await ws.close()
    return ws


def _index_factory(static_dir: Path) -> Callable[[web.Request], Any]:
    async def index(request: web.Request) -> web.FileResponse:
        return web.FileResponse(static_dir / "index.html")

    return index


async def serve(app: web.Application, host: str, port: int) -> web.AppRunner:
    """Start serving; caller owns shutdown via ``runner.cleanup()``."""
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    return runner
