"""Harvest pipeline: queue, job state machine, retry policy (design §3.5).

One worker per transfer adapter (v1: exactly one). Per job:
connect → SESSION_LIST → diff vs index → download each unknown session's
flight.txt to a spool ``.part`` (resuming any prior partial) → verify →
commit to staging with harvest-time jumper attribution → disconnect.

Retry policy: failures leave ``.part`` files in place and arm a retry that
fires on the device's *next sighting* (no blind timers against an absent
device), with a cooldown between attempts and at most ``max_attempts`` per
RETURNED trigger. All failures are loud, categorized events.

The worker never writes to a device: the link interface has no write methods
(protocol.py), and the contract suite asserts no write-shaped calls occur.
"""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from tempo_tb_ingest.device.protocol import (
    ConnectError,
    LinkDisconnected,
    LinkError,
    TempoDeviceLink,
    log_path,
)
from tempo_tb_ingest.events import (
    EventBus,
    HarvestCompleted,
    HarvestFailed,
    HarvestQueued,
    HarvestSessionList,
    HarvestStarted,
    HarvestTruncated,
    OwnersUnmapped,
    StoreSessionAdded,
    TransferCompleted,
    TransferFailed,
    TransferProgress,
    TransferStarted,
)
from tempo_tb_ingest.events import (
    StoreError as StoreErrorEvent,
)
from tempo_tb_ingest.owners import OwnersRegistry
from tempo_tb_ingest.store import Store, StoreError

#: resolve a device id to its current (name, mac-or-None) — from presence
ResolveTargetFn = Callable[[str], tuple[str, str | None] | None]
LinkFactoryFn = Callable[[str], TempoDeviceLink]  # address -> link


@dataclass
class _Pending:
    attempt: int  # attempts used so far this RETURNED cycle
    not_before: datetime  # cooldown gate for the sighting-driven retry


class HarvestWorker:
    def __init__(
        self,
        bus: EventBus,
        store: Store,
        owners: OwnersRegistry,
        link_factory: LinkFactoryFn,
        resolve_target: ResolveTargetFn,
        *,
        max_attempts: int = 5,
        retry_cooldown_s: float = 15.0,
        progress_interval_s: float = 0.2,  # ≤ 5 Hz per wire contract
        radio_lock: asyncio.Lock | None = None,
        clock: Callable[[], datetime] | None = None,
        on_harvested: Callable[[str], None] | None = None,
    ) -> None:
        self._bus = bus
        self._store = store
        self._owners = owners
        self._link_factory = link_factory
        self._resolve_target = resolve_target
        self._max_attempts = max_attempts
        self._retry_cooldown_s = retry_cooldown_s
        self._progress_interval_s = progress_interval_s
        self._radio_lock = radio_lock or asyncio.Lock()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._on_harvested = on_harvested
        self._queue: asyncio.Queue[tuple[str, int]] = asyncio.Queue()
        self._queued: set[str] = set()
        self._retry: dict[str, _Pending] = {}
        self._stop = asyncio.Event()

    # -- triggers (called by presence wiring) ----------------------------------

    def request(self, device_id: str) -> None:
        """A RETURNED trigger: fresh harvest cycle (attempt counter resets)."""
        self._retry.pop(device_id, None)
        self._enqueue(device_id, attempt=1)

    def notify_sighting(self, device_id: str) -> None:
        """Sighting-driven retry: re-queue a failed job once cooled down."""
        pending = self._retry.get(device_id)
        if pending is None or self._clock() < pending.not_before:
            return
        del self._retry[device_id]
        self._enqueue(device_id, attempt=pending.attempt + 1)

    def _enqueue(self, device_id: str, attempt: int) -> None:
        if device_id in self._queued:
            return  # coalesce
        self._queued.add(device_id)
        self._queue.put_nowait((device_id, attempt))
        self._bus.publish(HarvestQueued(id=device_id, attempt=attempt))

    # -- worker loop ------------------------------------------------------------

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                device_id, attempt = await asyncio.wait_for(self._queue.get(), timeout=0.5)
            except TimeoutError:
                continue
            self._queued.discard(device_id)
            await self._run_job(device_id, attempt)

    async def run_until_idle(self) -> None:
        """Drain the queue and return (tests, one-shot CLI harvests)."""
        while self._queued:
            device_id, attempt = self._queue.get_nowait()
            self._queued.discard(device_id)
            await self._run_job(device_id, attempt)

    # -- one job ------------------------------------------------------------------

    async def _run_job(self, device_id: str, attempt: int) -> None:
        self._bus.publish(HarvestStarted(id=device_id, attempt=attempt))
        target = self._resolve_target(device_id)
        if target is None:
            self._fail(device_id, attempt, "device no longer tracked", resumable=True)
            return
        name, mac = target
        link = self._link_factory(mac or name)

        started_at = self._clock()
        downloaded = 0
        downloaded_bytes = 0
        try:
            async with self._radio_lock:
                try:
                    await link.connect()
                except ConnectError as exc:
                    self._fail(device_id, attempt, f"connect: {exc}", resumable=True)
                    return
                try:
                    result = await link.session_list()
                    new_keys = self._store.new_sessions(device_id, result.keys)
                    self._bus.publish(
                        HarvestSessionList(
                            id=device_id,
                            count=len(result.keys),
                            new_count=len(new_keys),
                            truncated=result.truncated,
                        )
                    )
                    if result.truncated:
                        self._bus.publish(HarvestTruncated(id=device_id))

                    owner = self._owners.lookup(device_id)
                    if owner is None and new_keys:
                        self._bus.publish(OwnersUnmapped(id=device_id, name=name))

                    for index, key in enumerate(new_keys, start=1):
                        try:
                            await self._transfer(
                                link, device_id, name, mac, key, index, len(new_keys)
                            )
                        except LinkDisconnected:
                            raise  # connection dead: abort the job (retry later)
                        except (StoreError, LinkError):
                            # bad session (empty/vanished/corrupt transfer):
                            # loudly evented in _transfer; the rest of the
                            # harvest proceeds and this key retries next cycle
                            continue
                        downloaded += 1
                        downloaded_bytes += self._store.staging_path(device_id, key).stat().st_size
                finally:
                    await link.disconnect()
        except LinkDisconnected as exc:
            self._fail(device_id, attempt, f"disconnected: {exc}", resumable=True)
            return
        except (LinkError, StoreError) as exc:
            self._fail(device_id, attempt, str(exc), resumable=False)
            return

        duration = (self._clock() - started_at).total_seconds()
        self._bus.publish(
            HarvestCompleted(
                id=device_id,
                sessions_downloaded=downloaded,
                bytes=downloaded_bytes,
                duration_s=duration,
            )
        )
        if self._on_harvested is not None:
            self._on_harvested(device_id)

    async def _transfer(
        self,
        link: TempoDeviceLink,
        device_id: str,
        name: str,
        mac: str | None,
        key: str,
        index: int,
        total_files: int,
    ) -> None:
        path = log_path(key)
        spool = self._store.spool_path(device_id, key)
        offset = spool.stat().st_size if spool.exists() else 0

        self._bus.publish(
            TransferStarted(
                id=device_id,
                session_key=key,
                file_index=index,
                file_total=total_files,
                resumed_from=offset,
            )
        )

        emit_ts = self._clock()
        emit_bytes = offset

        def on_progress(done: int) -> None:
            nonlocal emit_ts, emit_bytes
            now = self._clock()
            elapsed = (now - emit_ts).total_seconds()
            if elapsed < self._progress_interval_s:
                return
            rate = (done - emit_bytes) / elapsed if elapsed > 0 else 0.0
            emit_ts = now
            emit_bytes = done
            self._bus.publish(
                TransferProgress(
                    id=device_id,
                    session_key=key,
                    bytes_done=done,
                    bytes_total=None,
                    rate_bps=rate,
                )
            )

        transfer_started = self._clock()
        try:
            with spool.open("ab") as sink:
                await link.download(path, sink, offset=offset, progress=on_progress)
        except LinkDisconnected:
            self._bus.publish(
                TransferFailed(
                    id=device_id, session_key=key, reason="disconnected", resumable=True
                )
            )
            raise

        # verify: the spool must now be exactly as large as the device says
        expected = await link.read_size(path)
        actual = spool.stat().st_size
        if actual != expected:
            self._bus.publish(
                TransferFailed(
                    id=device_id,
                    session_key=key,
                    reason=f"size mismatch: spool {actual} != device {expected}",
                    resumable=False,
                )
            )
            spool.unlink(missing_ok=True)  # do not resume from a corrupt partial
            raise LinkError(f"{device_id}/{key}: size mismatch")

        owner = self._owners.lookup(device_id)
        try:
            record = self._store.commit(
                device_id, key, spool, device_name=name, mac=mac, owner=owner
            )
        except StoreError as exc:
            self._bus.publish(StoreErrorEvent(id=device_id, session_key=key, reason=str(exc)))
            spool.unlink(missing_ok=True)
            raise

        duration = (self._clock() - transfer_started).total_seconds()
        self._bus.publish(
            TransferCompleted(
                id=device_id,
                session_key=key,
                bytes=record.size,
                sha256=record.sha256,
                duration_s=duration,
            )
        )
        self._bus.publish(
            StoreSessionAdded(
                id=device_id,
                session_key=key,
                path=record.path,
                size=record.size,
                sha256=record.sha256,
                jumper=record.jumper,
            )
        )

    def _fail(self, device_id: str, attempt: int, reason: str, *, resumable: bool) -> None:
        will_retry = resumable and attempt < self._max_attempts
        if will_retry:
            self._retry[device_id] = _Pending(
                attempt=attempt,
                not_before=self._clock() + timedelta(seconds=self._retry_cooldown_s),
            )
        self._bus.publish(
            HarvestFailed(id=device_id, reason=reason, attempt=attempt, will_retry=will_retry)
        )
