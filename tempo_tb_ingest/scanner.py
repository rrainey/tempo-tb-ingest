"""Continuous BLE observation → a stream of Sightings (design §3.2).

The scanner is deliberately policy-free: it filters advertisements down to
Tempo-BT candidates (SMP service UUID in the advertising packet, or a
Tempo-BT* name in the scan response) and emits ``Sighting`` records. All
presence/return logic lives in ``presence.py`` and never imports bleak.

Resilience: if the underlying scan fails (BlueZ restart, adapter reset), the
scanner publishes ``scanner.degraded``, retries with exponential backoff
(1 s → 30 s cap), and publishes ``scanner.recovered`` when scanning resumes.
The daemon never exits because scanning broke.

Scanning is *active* (BlueZ default) — device names arrive in scan responses,
and identity requires the name (design §3.3).
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from tempo_tb_ingest.events import EventBus, ScannerDegraded, ScannerRecovered

#: Standard Zephyr SMP service UUID, advertised by Tempo-BT firmware
#: (ble_mcumgr.c ad[]); the name is only in the scan response.
SMP_SERVICE_UUID = "8d53dc1d-1db7-4cd3-868b-8a527460aa84"

TEMPO_NAME_PREFIX = "Tempo-BT"

DEFAULT_QUEUE_SIZE = 1024


@dataclass(frozen=True)
class Sighting:
    """One filtered advertisement observation."""

    mac: str
    name: str | None  # None until the scan response supplies it
    rssi: int
    ts: datetime


def is_tempo_advertisement(name: str | None, service_uuids: list[str]) -> bool:
    if any(u.lower() == SMP_SERVICE_UUID for u in service_uuids):
        return True
    return name is not None and name.startswith(TEMPO_NAME_PREFIX)


#: A backend runs one scan session: call ``started`` once scanning is truly
#: on, deliver raw observations to ``detected`` until ``stop`` is set (normal
#: return), and raise on failure.
RawDetectionFn = Callable[[str, str | None, int, list[str]], None]
ScanBackend = Callable[[RawDetectionFn, asyncio.Event, Callable[[], None]], Awaitable[None]]


def bleak_backend(adapter: str | None = None) -> ScanBackend:
    """The real backend: bleak active scanning on the given adapter."""

    async def run(
        detected: RawDetectionFn, stop: asyncio.Event, started: Callable[[], None]
    ) -> None:
        from bleak import BleakScanner  # imported here: presence tests never need it

        def on_detection(device: object, adv: object) -> None:
            detected(
                str(getattr(device, "address", "")),
                getattr(adv, "local_name", None) or getattr(device, "name", None),
                int(getattr(adv, "rssi", 0)),
                list(getattr(adv, "service_uuids", []) or []),
            )

        kwargs: dict[str, object] = {"detection_callback": on_detection}
        if adapter is not None:
            kwargs["adapter"] = adapter
        scanner = BleakScanner(**kwargs)  # type: ignore[arg-type]
        async with scanner:
            started()
            await stop.wait()

    return run


class Scanner:
    """Runs scan sessions forever, emitting Sightings into a bounded queue."""

    def __init__(
        self,
        bus: EventBus,
        backend: ScanBackend,
        *,
        clock: Callable[[], datetime] | None = None,
        queue_size: int = DEFAULT_QUEUE_SIZE,
        backoff_initial_s: float = 1.0,
        backoff_max_s: float = 30.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._bus = bus
        self._backend = backend
        self._clock = clock or (lambda: datetime.now(UTC))
        self._queue: asyncio.Queue[Sighting] = asyncio.Queue(maxsize=queue_size)
        self._backoff_initial_s = backoff_initial_s
        self._backoff_max_s = backoff_max_s
        self._sleep = sleep
        self._stop = asyncio.Event()
        self._paused = False
        self._resume = asyncio.Event()
        self._session_stop: asyncio.Event | None = None
        self._idle = asyncio.Event()  # set whenever no scan session is active
        self._idle.set()
        self.dropped = 0  # sightings shed on overflow (periodic data; safe to shed)

    # -- producing ----------------------------------------------------------

    def _on_raw_detection(
        self, mac: str, name: str | None, rssi: int, service_uuids: list[str]
    ) -> None:
        if not is_tempo_advertisement(name, service_uuids):
            return
        sighting = Sighting(mac=mac, name=name, rssi=rssi, ts=self._clock())
        while True:
            try:
                self._queue.put_nowait(sighting)
                return
            except asyncio.QueueFull:
                with contextlib.suppress(asyncio.QueueEmpty):
                    self._queue.get_nowait()
                    self.dropped += 1

    async def run(self) -> None:
        """Scan until stop(); backoff-restart on backend failure."""
        backoff = self._backoff_initial_s
        degraded_since: datetime | None = None

        def on_started() -> None:
            nonlocal degraded_since, backoff
            backoff = self._backoff_initial_s
            if degraded_since is not None:
                outage = (self._clock() - degraded_since).total_seconds()
                self._bus.publish(ScannerRecovered(outage_s=outage))
                degraded_since = None

        while not self._stop.is_set():
            if self._paused:
                await self._resume.wait()
                continue
            session_stop = asyncio.Event()
            self._session_stop = session_stop
            self._idle.clear()
            try:
                await self._backend(self._on_raw_detection, session_stop, on_started)
            except Exception as exc:
                if degraded_since is None:
                    degraded_since = self._clock()
                self._bus.publish(ScannerDegraded(reason=f"{type(exc).__name__}: {exc}"))
                self._idle.set()
                await self._sleep(backoff)
                backoff = min(backoff * 2, self._backoff_max_s)
            else:
                self._idle.set()

    def stop(self) -> None:
        self._stop.set()
        self._resume.set()
        if self._session_stop is not None:
            self._session_stop.set()

    async def pause(self) -> None:
        """Suspend scanning and wait until the scan session has fully ended.

        BlueZ rejects connections while discovery is active
        (org.bluez.Error.InProgress — observed live 2026-07-08), so the
        harvest worker pauses the scanner for the duration of each
        connection. Not an outage: no degraded/recovered events.
        """
        self._paused = True
        self._resume = asyncio.Event()
        if self._session_stop is not None:
            self._session_stop.set()
        await self._idle.wait()

    def resume(self) -> None:
        self._paused = False
        self._resume.set()

    # -- consuming ----------------------------------------------------------

    async def sightings(self) -> AsyncIterator[Sighting]:
        """The AdvertisementSource: an async stream of filtered sightings."""
        while not (self._stop.is_set() and self._queue.empty()):
            try:
                yield await asyncio.wait_for(self._queue.get(), timeout=0.5)
            except TimeoutError:
                continue
