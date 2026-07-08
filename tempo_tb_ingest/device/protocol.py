"""The device link interface (design §3.1) — everything harvest needs from a device.

Two implementations: ``smp_link`` (real, smpclient over BLE) and ``fake_link``
(scripted, for the offline test tiers). One behavior spec — the contract test
suite in tests/test_link_contract.py — runs against both.

The interface is deliberately read-only: v1 harvesting never writes to a
device (CLAUDE.md). The lone exception surface, session deletion, is not part
of this interface at all.
"""

import re
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import IO, Self

SESSION_KEY_RE = re.compile(r"^\d{8}/[0-9A-F]{8}$")

#: Device path of a session's log file, from its session key (design §3.3/§6).
LOG_PATH_TEMPLATE = "/SD:/logs/{key}/flight.txt"

#: Marker file that designates an SD card as test/scratch media (CLAUDE.md).
TESTOK_PATH = "/SD:/testok"


def log_path(session_key: str) -> str:
    if not SESSION_KEY_RE.match(session_key):
        raise ValueError(f"malformed session key {session_key!r}")
    return LOG_PATH_TEMPLATE.format(key=session_key)


class LinkError(Exception):
    """Base for all device-link failures."""


class ConnectError(LinkError):
    """Could not establish (or lost while establishing) a connection."""


class LinkDisconnected(LinkError):
    """Connection dropped mid-operation; offset-based resume is possible."""


class FileNotFoundOnDevice(LinkError):
    """The requested path does not exist on the device."""


class FileIsDirectory(LinkError):
    """The requested path is a directory (fs STATUS rc=4)."""


@dataclass
class SessionListResult:
    keys: list[str]  # "<YYYYMMDD>/<8HEX>", device order preserved
    truncated: bool

    def __post_init__(self) -> None:
        bad = [k for k in self.keys if not SESSION_KEY_RE.match(k)]
        if bad:
            raise ValueError(f"malformed session keys from device: {bad!r}")


@dataclass
class StorageInfo:
    backend: str
    free_bytes: int
    total_bytes: int
    used_percent: int


ProgressFn = Callable[[int], None]  # called with cumulative bytes transferred


@dataclass
class LinkCallLog:
    """Every interface call, recorded — lets tests assert 'never wrote'."""

    calls: list[str] = field(default_factory=list)

    def note(self, call: str) -> None:
        self.calls.append(call)


class TempoDeviceLink(ABC):
    """One connected conversation with a Tempo-BT device."""

    call_log: LinkCallLog

    @abstractmethod
    async def connect(self) -> None:
        """Establish the connection (incl. MTU exchange). Raises ConnectError."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Tear down; never raises."""

    @abstractmethod
    async def session_list(self) -> SessionListResult:
        """Custom group-64 SESSION_LIST (feasibility §firmware, design §6)."""

    @abstractmethod
    async def storage_info(self) -> StorageInfo:
        """Custom group-64 STORAGE_INFO."""

    @abstractmethod
    async def read_size(self, path: str) -> int:
        """SMP fs STATUS. Raises FileNotFoundOnDevice / FileIsDirectory."""

    @abstractmethod
    async def download(
        self,
        path: str,
        sink: IO[bytes],
        *,
        offset: int = 0,
        progress: ProgressFn | None = None,
    ) -> int:
        """SMP fs download from ``offset``, appending to ``sink``.

        Returns bytes written to ``sink`` (i.e. total - offset on success).
        Raises LinkDisconnected mid-transfer (sink keeps the partial bytes),
        FileNotFoundOnDevice for a missing path.
        """

    async def probe_testok(self) -> bool:
        """True iff the card carries the /SD:/testok marker (CLAUDE.md).

        A directory named testok also counts as marked — the marker's meaning
        is 'this card is scratch media', and only a human creates it.
        """
        try:
            await self.read_size(TESTOK_PATH)
        except FileNotFoundOnDevice:
            return False
        except FileIsDirectory:
            return True
        return True

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.disconnect()
