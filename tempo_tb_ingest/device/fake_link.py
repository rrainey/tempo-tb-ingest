"""Scripted in-memory device link for the offline test tiers (design §3.1).

Behaves like a Tempo-BT as characterized on live hardware (phase 0/0.5 and
step 7 fault characterization): a scripted filesystem of device paths, the
group-64 session list derived from it, configurable chunking, and
fault-injection hooks:

- ``connect_failures``: the first N connect() calls raise ConnectError
  (models the observed miss-discovery-right-after-disconnect behavior).
- ``drop_at``: {path: byte_offset} — one-shot mid-download disconnect after
  the sink has received exactly that many *new* bytes (partial bytes stay in
  the sink, as with a real radio drop).
- ``truncated``: SESSION_LIST reports a truncated listing.

Every interface call is recorded in ``call_log`` so tests can assert what
did (and did not — e.g. group-64 writes) happen.
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import IO

from tempo_tb_ingest.device.protocol import (
    ConnectError,
    FileIsDirectory,
    FileNotFoundOnDevice,
    LinkCallLog,
    LinkDisconnected,
    LinkError,
    ProgressFn,
    SessionListResult,
    StorageInfo,
    TempoDeviceLink,
    log_path,
)

DEFAULT_CHUNK = 1024  # matches CONFIG_MCUMGR_GRP_FS_DL_CHUNK_SIZE (fw v1.5.0)


class FakeLink(TempoDeviceLink):
    """One scripted device; construct per test with its filesystem contents."""

    def __init__(
        self,
        *,
        sessions: dict[str, bytes] | None = None,
        extra_files: dict[str, bytes] | None = None,
        directories: set[str] | None = None,
        truncated: bool = False,
        connect_failures: int = 0,
        drop_at: dict[str, int] | None = None,
        chunk_size: int = DEFAULT_CHUNK,
        pause: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.call_log = LinkCallLog()
        self._sessions = dict(sessions or {})
        self._files: dict[str, bytes] = {}
        for key, content in self._sessions.items():
            self._files[log_path(key)] = content
        self._files.update(extra_files or {})
        self._directories = set(directories or set())
        self._truncated = truncated
        self._connect_failures = connect_failures
        self._drop_at = dict(drop_at or {})
        self._chunk_size = chunk_size
        self._pause = pause
        self._connected = False
        self.connect_attempts = 0

    # -- scripting helpers -------------------------------------------------

    def add_session(self, key: str, content: bytes) -> None:
        """Sessions can appear between harvests (a new jump landed)."""
        self._sessions[key] = content
        self._files[log_path(key)] = content

    def mark_testok(self, content: bytes = b"") -> None:
        self._files["/SD:/testok"] = content

    # -- TempoDeviceLink ----------------------------------------------------

    async def connect(self) -> None:
        self.call_log.note("connect")
        self.connect_attempts += 1
        await self._maybe_pause()
        if self._connect_failures > 0:
            self._connect_failures -= 1
            raise ConnectError("fake: device not found")
        self._connected = True

    async def disconnect(self) -> None:
        self.call_log.note("disconnect")
        self._connected = False

    async def session_list(self) -> SessionListResult:
        self.call_log.note("session_list")
        self._require_connected()
        await self._maybe_pause()
        # Firmware >= 1.6.0 order: date directory descending, id ascending
        keys = sorted(self._sessions)
        keys.sort(key=lambda k: k.split("/", 1)[0], reverse=True)
        return SessionListResult(keys=keys, truncated=self._truncated)

    async def storage_info(self) -> StorageInfo:
        self.call_log.note("storage_info")
        self._require_connected()
        await self._maybe_pause()
        used = sum(len(c) for c in self._files.values())
        total = 31_086_084_096
        return StorageInfo(
            backend="sdcard",
            free_bytes=total - used,
            total_bytes=total,
            used_percent=int(used * 100 / total),
        )

    async def read_size(self, path: str) -> int:
        self.call_log.note(f"read_size {path}")
        self._require_connected()
        await self._maybe_pause()
        if path in self._directories:
            raise FileIsDirectory(path)
        if path not in self._files:
            raise FileNotFoundOnDevice(path)
        return len(self._files[path])

    async def download(
        self,
        path: str,
        sink: IO[bytes],
        *,
        offset: int = 0,
        progress: ProgressFn | None = None,
    ) -> int:
        self.call_log.note(f"download {path} offset={offset}")
        self._require_connected()
        if path not in self._files:
            raise FileNotFoundOnDevice(path)
        content = self._files[path]
        if offset > len(content):
            raise LinkError(f"fake: offset {offset} beyond end of {path}")

        drop_after = self._drop_at.pop(path, None)  # one-shot
        written = 0
        position = offset
        while position < len(content):
            await self._maybe_pause()
            chunk = content[position : position + self._chunk_size]
            if drop_after is not None and written + len(chunk) >= drop_after:
                keep = drop_after - written
                sink.write(chunk[:keep])
                written += keep
                self._connected = False
                if progress is not None:
                    progress(offset + written)
                raise LinkDisconnected(f"fake: dropped after {written} bytes of {path}")
            sink.write(chunk)
            written += len(chunk)
            position += len(chunk)
            if progress is not None:
                progress(offset + written)
        return written

    # -- internals ----------------------------------------------------------

    def _require_connected(self) -> None:
        if not self._connected:
            raise LinkDisconnected("fake: not connected")

    async def _maybe_pause(self) -> None:
        if self._pause is not None:
            await self._pause()
        else:
            await asyncio.sleep(0)
