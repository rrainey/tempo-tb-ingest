"""Daemon assembly: scanner → presence → harvest → bus → API/recorder (§3).

Composition and lifecycle only — every component is injected or built from
config, so the whole daemon runs against fakes in tests. Guarantees:

- single instance per data_dir (flock on ``daemon.lock``);
- graceful shutdown: in-flight transfer aborts cleanly (spool ``.part``
  retained for resume), ``daemon.stopping`` is published, the recorder
  flushes, subscribers close;
- the live snapshot merges authoritative component state (presence devices,
  store session counts) with event-derived dynamics (queue/active job/totals
  from a StateFold) — one coherent §6.1 structure.

The scanner keeps running during transfers in v1: BlueZ merges discovery
sessions, and bleak's connect path scans anyway. Field-trial telemetry
(step 16) revisits this if link reliability suffers.
"""

import asyncio
import contextlib
import fcntl
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import IO, Any

from tempo_tb_ingest import __version__
from tempo_tb_ingest.config import Config
from tempo_tb_ingest.device.protocol import TempoDeviceLink
from tempo_tb_ingest.events import DaemonStarted, DaemonStopping, EventBus
from tempo_tb_ingest.harvest import HarvestWorker
from tempo_tb_ingest.owners import OwnersRegistry
from tempo_tb_ingest.presence import PresenceTracker, device_id_from_name
from tempo_tb_ingest.recorder import Recorder
from tempo_tb_ingest.scanner import ScanBackend, Scanner, bleak_backend
from tempo_tb_ingest.statefold import StateFold
from tempo_tb_ingest.store import Store

logger = logging.getLogger(__name__)

SWEEP_INTERVAL_S = 5.0


class AlreadyRunning(Exception):
    """Another daemon instance owns this data_dir."""


class ScannerPausingRadioGate:
    """The daemon's radio gate: serialize connections AND pause discovery.

    BlueZ refuses to connect while discovery is active
    (org.bluez.Error.InProgress, observed live 2026-07-08) — so each
    connected job suspends the scanner for its duration."""

    def __init__(self, scanner: Scanner) -> None:
        self._scanner = scanner
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> None:
        await self._lock.acquire()
        await self._scanner.pause()

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        self._scanner.resume()
        self._lock.release()


class Daemon:
    def __init__(
        self,
        config: Config,
        *,
        scan_backend: ScanBackend | None = None,
        link_factory: Callable[[str], TempoDeviceLink] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config
        self._clock = clock or (lambda: datetime.now(UTC))
        self._lock_file: IO[str] | None = None

        self.bus = EventBus(clock=self._clock)
        self.recorder = Recorder(self.bus, config.store.data_dir / "events")
        self.store = Store(
            staging_root=config.store.staging_root,
            data_dir=config.store.data_dir,
            spool_dir=config.harvest.spool_dir,
            bus=self.bus,
        )
        self.owners = OwnersRegistry(config.store.resolved_owners_file(), self.bus)
        self.scanner = Scanner(
            self.bus,
            scan_backend or bleak_backend(config.adapter.scan),
            clock=self._clock,
        )
        self.worker = HarvestWorker(
            self.bus,
            self.store,
            self.owners,
            link_factory or self._default_link_factory,
            resolve_target=self._resolve_target,
            max_attempts=config.harvest.max_attempts,
            radio_lock=ScannerPausingRadioGate(self.scanner),
            clock=self._clock,
            on_harvested=self._on_harvested,
        )
        self.presence = PresenceTracker(
            self.bus,
            rssi_floor_dbm=config.detection.rssi_floor_dbm,
            lost_after_s=config.detection.lost_after_s,
            absent_after_s=config.detection.absent_after_s,
            on_returned=self.worker.request,
        )
        # event-derived dynamics for the snapshot (queue/active job/totals/warnings)
        self._fold = StateFold(
            version=__version__,
            adapters={
                "scan": config.adapter.scan,
                "transfer": list(config.adapter.transfer),
            },
        )
        self._fold_subscription = self.bus.subscribe(queue_size=4096)
        self._stopping = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []

    # -- wiring helpers -------------------------------------------------------

    def _default_link_factory(self, address: str) -> TempoDeviceLink:
        from tempo_tb_ingest.device.smp_link import SmpLink

        return SmpLink(address, connect_timeout_s=self.config.harvest.connect_timeout_s)

    def _resolve_target(self, device_id: str) -> tuple[str, str | None] | None:
        for record in self.presence.devices():
            if record.id == device_id:
                return (record.name, record.mac)
        return None

    def _on_harvested(self, device_id: str) -> None:
        self.presence.mark_harvested(device_id, self._clock())

    # -- snapshot ---------------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """§6.1: authoritative components + event-derived dynamics."""
        folded = self._fold.snapshot(now=self._clock())
        devices: list[dict[str, Any]] = []
        for record in sorted(self.presence.devices(), key=lambda r: r.id):
            folded_dev: dict[str, Any] = next(
                (d for d in folded["devices"] if d["id"] == record.id), {}
            )
            devices.append(
                {
                    "id": record.id,
                    "name": record.name,
                    "folder": f"TempoBT-{record.id}",
                    "mac": record.mac,
                    "jumper": folded_dev.get("jumper"),
                    "is_lo": folded_dev.get("is_lo", False),
                    "state": record.state.value,
                    "rssi": record.rssi,
                    "last_seen": record.last_seen.isoformat(),
                    "away_since": (record.away_since.isoformat() if record.away_since else None),
                    "sessions_known": len(self.store.known_sessions(record.id)),
                    "provisioning_needed": False,
                    "conflicted": record.conflicted,
                    "truncated": folded_dev.get("truncated", False),
                }
            )
        for up in sorted(self.presence.unprovisioned(), key=lambda u: u.mac):
            devices.append(
                {
                    "id": None,
                    "name": up.name,
                    "mac": up.mac,
                    "provisioning_needed": True,
                    "last_seen": up.last_seen.isoformat(),
                }
            )
        count, size = self.store.totals()
        totals = dict(folded["totals"])
        totals["sessions_stored"] = count
        totals["bytes_stored"] = size
        return {
            "v": 1,
            "seq": self.bus.last_seq,
            "ts": folded["ts"],
            "daemon": folded["daemon"],
            "devices": devices,
            "queue": folded["queue"],
            "active_job": folded["active_job"],
            "totals": totals,
        }

    # -- lifecycle ---------------------------------------------------------------

    def _acquire_lock(self) -> None:
        lock_path = self.config.store.data_dir / "daemon.lock"
        lock_file = lock_path.open("w")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            lock_file.close()
            raise AlreadyRunning(f"another daemon owns {lock_path}") from exc
        self._lock_file = lock_file

    def _release_lock(self) -> None:
        if self._lock_file is not None:
            with contextlib.suppress(OSError):
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
            self._lock_file.close()
            self._lock_file = None

    async def run(self) -> None:
        """Run until stop(); raises AlreadyRunning if the data_dir is owned."""
        self._acquire_lock()
        runner = None
        try:
            self.bus.publish(
                DaemonStarted(
                    version=__version__,
                    config=self.config.model_dump(mode="json"),
                )
            )
            from tempo_tb_ingest.api import create_app, serve

            app = create_app(self.bus, self.snapshot)
            runner = await serve(app, self.config.http.host, self.config.http.port)

            # the recorder is not in _tasks: it must never be cancelled —
            # it drains naturally when the bus closes, so shutdown events
            # (incl. daemon.stopping) always reach disk
            self._recorder_task = asyncio.create_task(self.recorder.run(), name="recorder")
            self._tasks = [
                asyncio.create_task(self.scanner.run(), name="scanner"),
                asyncio.create_task(self.worker.run(), name="worker"),
                asyncio.create_task(self._pump_sightings(), name="sightings"),
                asyncio.create_task(self._pump_fold(), name="fold"),
                asyncio.create_task(self._sweep_loop(), name="sweep"),
            ]
            await self._stopping.wait()
        finally:
            await self._shutdown(runner)

    def stop(self, reason: str = "shutdown requested") -> None:
        if not self._stopping.is_set():
            self._stop_reason = reason
            self._stopping.set()

    async def _shutdown(self, runner: Any) -> None:
        reason = getattr(self, "_stop_reason", "shutdown requested")
        logger.info("stopping: %s", reason)
        self.scanner.stop()
        self.worker.stop()
        self.bus.publish(DaemonStopping(reason=reason))
        # cancel work loops; an in-flight transfer aborts here — its spool
        # .part stays on disk and resumes on the next attempt
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self.bus.close()
        # the recorder drains the closed bus (daemon.stopping included)
        recorder_task = getattr(self, "_recorder_task", None)
        if recorder_task is not None:
            with contextlib.suppress(Exception):
                await recorder_task
        if runner is not None:
            await runner.cleanup()
        self.store.close()
        self._release_lock()

    # -- internal loops -------------------------------------------------------------

    async def _pump_sightings(self) -> None:
        async for sighting in self.scanner.sightings():
            self.presence.observe(sighting)
            device_id = device_id_from_name(sighting.name)
            if device_id is not None:
                self.worker.notify_sighting(device_id)

    async def _pump_fold(self) -> None:
        async for env in self._fold_subscription:
            self._fold.apply(env)

    async def _sweep_loop(self) -> None:
        while True:
            await asyncio.sleep(SWEEP_INTERVAL_S)
            self.presence.sweep(self._clock())
