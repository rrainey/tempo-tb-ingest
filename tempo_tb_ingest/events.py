"""Event model and in-process bus (design §3.7, wire contract §6.2).

Every event on the wire is an ``Envelope``: ``{v, seq, ts, type, data}``.
``seq`` is a per-daemon-run monotonic counter assigned by the bus; ``ts`` is
ISO-8601 UTC with millisecond precision and a ``Z`` suffix. ``data`` payloads
are the typed models below, registered by their ``type`` string.

Subscribers consume through bounded queues. A slow subscriber never blocks a
publisher: the oldest queued event is dropped and the subscriber receives a
synthetic ``stream.gap`` envelope (per-subscriber, ``seq = -1``, not part of
the global sequence) telling it to re-snapshot.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from typing import Any, ClassVar, Self

from pydantic import BaseModel, ConfigDict, field_serializer

DEFAULT_SUBSCRIBER_QUEUE = 256


def _utcnow() -> datetime:
    return datetime.now(UTC)


class EventError(Exception):
    """Unknown event type or malformed envelope."""


# --------------------------------------------------------------------------- #
# data payloads

EVENT_TYPES: dict[str, type["EventData"]] = {}


class EventData(BaseModel):
    """Base for all event payloads; subclasses set ``TYPE`` and are registered."""

    model_config = ConfigDict(extra="forbid")

    TYPE: ClassVar[str] = ""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls.TYPE:
            raise TypeError(f"{cls.__name__} must define TYPE")
        if cls.TYPE in EVENT_TYPES:
            raise TypeError(f"duplicate event type {cls.TYPE!r}")
        EVENT_TYPES[cls.TYPE] = cls


class DaemonStarted(EventData):
    TYPE: ClassVar[str] = "daemon.started"
    version: str
    config: dict[str, Any]


class DaemonStopping(EventData):
    TYPE: ClassVar[str] = "daemon.stopping"
    reason: str


class ScannerDegraded(EventData):
    TYPE: ClassVar[str] = "scanner.degraded"
    reason: str


class ScannerRecovered(EventData):
    TYPE: ClassVar[str] = "scanner.recovered"
    outage_s: float


class DeviceSeen(EventData):
    TYPE: ClassVar[str] = "device.seen"
    id: str
    mac: str
    name: str
    rssi: int


class DeviceNew(EventData):
    TYPE: ClassVar[str] = "device.new"
    id: str
    mac: str
    name: str
    rssi: int


class DeviceAway(EventData):
    TYPE: ClassVar[str] = "device.away"
    id: str
    away_since: datetime

    @field_serializer("away_since")
    def _ser_away_since(self, value: datetime) -> str:
        return format_ts(value)


class DeviceReturned(EventData):
    TYPE: ClassVar[str] = "device.returned"
    id: str
    absent_for_s: float | None  # None = first-ever sighting


class DeviceLost(EventData):
    TYPE: ClassVar[str] = "device.lost"
    id: str


class DeviceProvisioningNeeded(EventData):
    TYPE: ClassVar[str] = "device.provisioning_needed"
    mac: str
    name: str


class DeviceIdentityConflict(EventData):
    TYPE: ClassVar[str] = "device.identity_conflict"
    id: str
    macs: list[str]


class HarvestQueued(EventData):
    TYPE: ClassVar[str] = "harvest.queued"
    id: str
    attempt: int


class HarvestStarted(EventData):
    TYPE: ClassVar[str] = "harvest.started"
    id: str
    attempt: int


class HarvestSessionList(EventData):
    TYPE: ClassVar[str] = "harvest.session_list"
    id: str
    count: int
    new_count: int
    truncated: bool


class HarvestTruncated(EventData):
    TYPE: ClassVar[str] = "harvest.truncated"
    id: str


class TransferStarted(EventData):
    TYPE: ClassVar[str] = "transfer.started"
    id: str
    session_key: str
    file_index: int
    file_total: int
    resumed_from: int  # byte offset; 0 = fresh download


class TransferProgress(EventData):
    TYPE: ClassVar[str] = "transfer.progress"
    id: str
    session_key: str
    bytes_done: int
    bytes_total: int | None
    rate_bps: float


class TransferCompleted(EventData):
    TYPE: ClassVar[str] = "transfer.completed"
    id: str
    session_key: str
    bytes: int
    sha256: str
    duration_s: float


class TransferFailed(EventData):
    TYPE: ClassVar[str] = "transfer.failed"
    id: str
    session_key: str
    reason: str
    resumable: bool


class StoreSessionAdded(EventData):
    TYPE: ClassVar[str] = "store.session_added"
    id: str
    session_key: str
    path: str
    size: int
    sha256: str
    jumper: str | None  # None = unmapped in device-owners.json


class StoreDuplicateHash(EventData):
    TYPE: ClassVar[str] = "store.duplicate_hash"
    id: str
    session_key: str
    sha256: str
    duplicate_of: str  # "device_id/session_key" already holding this content


class StoreError(EventData):
    TYPE: ClassVar[str] = "store.error"
    id: str
    session_key: str | None
    reason: str


class OwnersReloaded(EventData):
    TYPE: ClassVar[str] = "owners.reloaded"
    entries: int
    path: str


class OwnersError(EventData):
    TYPE: ClassVar[str] = "owners.error"
    reason: str
    path: str


class OwnersUnmapped(EventData):
    TYPE: ClassVar[str] = "owners.unmapped"
    id: str
    name: str


class HarvestCompleted(EventData):
    TYPE: ClassVar[str] = "harvest.completed"
    id: str
    sessions_downloaded: int
    bytes: int
    duration_s: float


class HarvestFailed(EventData):
    TYPE: ClassVar[str] = "harvest.failed"
    id: str
    reason: str
    attempt: int
    will_retry: bool


class StreamGap(EventData):
    TYPE: ClassVar[str] = "stream.gap"
    dropped_count: int


# --------------------------------------------------------------------------- #
# envelope


def format_ts(value: datetime) -> str:
    """ISO-8601 UTC, millisecond precision, Z suffix (wire contract §6.2)."""
    value = value.astimezone(UTC)
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"


class Envelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    v: int = 1
    seq: int
    ts: datetime
    type: str
    data: dict[str, Any]

    @field_serializer("ts")
    def _ser_ts(self, value: datetime) -> str:
        return format_ts(value)

    def payload(self) -> EventData:
        """Validate and return ``data`` as its registered typed model."""
        model = EVENT_TYPES.get(self.type)
        if model is None:
            raise EventError(f"unknown event type {self.type!r}")
        try:
            return model.model_validate(self.data)
        except ValueError as exc:
            raise EventError(f"malformed {self.type!r} payload: {exc}") from exc

    @classmethod
    def from_json(cls, line: str) -> Self:
        try:
            env = cls.model_validate_json(line)
        except ValueError as exc:
            raise EventError(f"malformed envelope: {exc}") from exc
        env.payload()  # raises EventError on unknown type / bad payload shape
        return env

    def to_json(self) -> str:
        return self.model_dump_json()


# --------------------------------------------------------------------------- #
# bus

_CLOSE_SENTINEL_SEQ = -(2**31)


class Subscription:
    """One subscriber's bounded view of the stream (async-iterable)."""

    def __init__(self, queue_size: int, clock: Callable[[], datetime]) -> None:
        if queue_size < 1:
            raise ValueError("queue_size must be >= 1")
        self._queue: asyncio.Queue[Envelope] = asyncio.Queue(maxsize=queue_size)
        self._dropped = 0
        self._clock = clock
        self._closed = False

    def _offer(self, env: Envelope) -> None:
        while True:
            try:
                self._queue.put_nowait(env)
                return
            except asyncio.QueueFull:
                try:
                    self._queue.get_nowait()  # drop-oldest
                    self._dropped += 1
                except asyncio.QueueEmpty:  # pragma: no cover
                    pass

    def _close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # A reader can only be blocked when the queue is empty, so the wake-up
        # sentinel is needed (and possible) exactly when there is space; on a
        # non-empty queue the reader drains it and then observes _closed.
        with contextlib.suppress(asyncio.QueueFull):
            self._queue.put_nowait(
                Envelope(
                    seq=_CLOSE_SENTINEL_SEQ,
                    ts=self._clock(),
                    type=StreamGap.TYPE,
                    data={"dropped_count": 0},
                )
            )

    def __aiter__(self) -> AsyncIterator[Envelope]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Envelope]:
        while True:
            if self._dropped:
                dropped, self._dropped = self._dropped, 0
                yield Envelope(
                    seq=-1,  # synthetic, per-subscriber; client must re-snapshot
                    ts=self._clock(),
                    type=StreamGap.TYPE,
                    data=StreamGap(dropped_count=dropped).model_dump(),
                )
            if self._closed and self._queue.empty():
                return
            env = await self._queue.get()
            if env.seq == _CLOSE_SENTINEL_SEQ:
                continue  # close sentinel: loop re-checks closed/empty state
            yield env


class EventBus:
    """In-process pub/sub; the single source of the daemon's event stream."""

    def __init__(
        self,
        clock: Callable[[], datetime] | None = None,
        queue_size: int = DEFAULT_SUBSCRIBER_QUEUE,
    ) -> None:
        self._clock = clock or _utcnow
        self._queue_size = queue_size
        self._subscribers: list[Subscription] = []
        self._seq = 0

    @property
    def last_seq(self) -> int:
        return self._seq

    def publish(self, data: EventData) -> Envelope:
        """Wrap ``data`` in an envelope (assigning seq/ts) and fan out."""
        self._seq += 1
        env = Envelope(seq=self._seq, ts=self._clock(), type=data.TYPE, data=data.model_dump())
        self._fan_out(env)
        return env

    def publish_envelope(self, env: Envelope) -> None:
        """Fan out a pre-built envelope verbatim (replay path, design §3.8)."""
        env.payload()  # refuse unknown/malformed events
        self._seq = max(self._seq, env.seq)
        self._fan_out(env)

    def _fan_out(self, env: Envelope) -> None:
        for sub in self._subscribers:
            sub._offer(env)

    def subscribe(self, queue_size: int | None = None) -> Subscription:
        sub = Subscription(queue_size or self._queue_size, self._clock)
        self._subscribers.append(sub)
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        if sub in self._subscribers:
            self._subscribers.remove(sub)
            sub._close()

    def close(self) -> None:
        """End all subscriptions (daemon shutdown)."""
        for sub in list(self._subscribers):
            self.unsubscribe(sub)
