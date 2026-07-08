"""Real TempoDeviceLink over SMP/BLE via smpclient → bleak → BlueZ (design §3.1).

Implementation notes:

- The chunked download loop is driven here (not ``SMPClient.download_file``)
  because harvest needs offset resume and progress callbacks. The fs download
  response carries the file's total length only when ``off == 0``, so a
  resumed download asks ``FileStatus`` for the total first.
- ``address`` may be a BLE name ("Tempo-BT-0001") or a MAC; the harvest
  worker passes the most recently sighted MAC (design §3.3) and falls back to
  the name.
- Adapter selection for multi-dongle pools (radio Option 2) is not yet
  plumbed through smpclient's BLE transport; single-adapter v1 uses the
  system default. Tracked for step 16+.
- Exception mapping to the LinkError taxonomy is centralized here; step 7
  (fault characterization) refines it against real radio drops.
"""

import asyncio
import contextlib
from typing import IO, Any

from smpclient import SMPClient
from smpclient.generics import error, error_v1, error_v2, success
from smpclient.requests.file_management import FileDownload, FileStatus
from smpclient.transport import SMPTransportDisconnected
from smpclient.transport.ble import SMPBLETransport

from tempo_tb_ingest.device import tempo_group as tg
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
)

FS_ERR_FILE_NOT_FOUND = 3  # FS_MGMT_ERR.FILE_NOT_FOUND (verified live 2026-07-08)
FS_ERR_FILE_IS_DIRECTORY = 4  # FS_MGMT_ERR.FILE_IS_DIRECTORY


class SmpLink(TempoDeviceLink):
    """One BLE connection to a Tempo-BT device."""

    def __init__(
        self,
        address: str,
        *,
        connect_timeout_s: float = 20.0,
        request_timeout_s: float = 10.0,
    ) -> None:
        self.call_log = LinkCallLog()
        self._address = address
        self._connect_timeout_s = connect_timeout_s
        self._client = SMPClient(SMPBLETransport(), address, timeout_s=request_timeout_s)

    async def connect(self) -> None:
        self.call_log.note("connect")
        try:
            await self._client.connect(self._connect_timeout_s)
        except LinkError:
            raise
        except Exception as exc:
            raise ConnectError(f"{self._address}: {exc}") from exc

    async def disconnect(self) -> None:
        self.call_log.note("disconnect")
        # interface contract: disconnect never raises
        with contextlib.suppress(Exception):
            await self._client.disconnect()

    async def session_list(self) -> SessionListResult:
        self.call_log.note("session_list")
        response = await self._request(tg.SessionList())
        return tg.session_list_result(response)

    async def storage_info(self) -> StorageInfo:
        self.call_log.note("storage_info")
        response = await self._request(tg.StorageInfo())
        return tg.storage_info_result(response)

    async def read_size(self, path: str) -> int:
        self.call_log.note(f"read_size {path}")
        response = await self._request(FileStatus(name=path))
        return int(response.len)

    async def download(
        self,
        path: str,
        sink: IO[bytes],
        *,
        offset: int = 0,
        progress: ProgressFn | None = None,
    ) -> int:
        self.call_log.note(f"download {path} offset={offset}")
        total: int | None = None
        if offset > 0:
            total = await self.read_size(path)
            if offset > total:
                raise LinkError(f"offset {offset} beyond end of {path} ({total} bytes)")
            if offset == total:
                return 0

        position = offset
        written = 0
        while True:
            response = await self._request(FileDownload(off=position, name=path))
            if position == 0 and response.len is not None:
                total = int(response.len)
            data = bytes(response.data)
            if not data and (total is None or position < total):
                raise LinkError(f"device returned empty chunk at offset {position} of {path}")
            sink.write(data)
            written += len(data)
            position += len(data)
            if progress is not None:
                progress(position)
            if total is None:
                raise LinkError(f"device never reported a length for {path}")
            if position >= total:
                return written

    # -- internals ----------------------------------------------------------

    async def _request(self, request: Any) -> Any:
        """Send one SMP request; map errors into the LinkError taxonomy."""
        try:
            response = await self._client.request(request)
        except SMPTransportDisconnected as exc:
            # the characterized mid-transfer failure mode (step 7, 2026-07-08):
            # link drop -> SMPTransportDisconnected; sink keeps a byte-exact,
            # chunk-aligned prefix and offset-resume completes identically
            raise LinkDisconnected(f"{self._address}: {exc}") from exc
        except TimeoutError as exc:
            raise LinkDisconnected(f"{self._address}: request timed out: {exc}") from exc
        except LinkError:
            raise
        except Exception as exc:
            # conservative catch-all: unknown transport failures are treated
            # as resumable disconnects rather than crashing the daemon
            raise LinkDisconnected(f"{self._address}: {exc}") from exc

        if success(response):
            return response
        if error(response):
            raise self._map_error(response)
        raise LinkError(f"unrecognized response: {response!r}")  # pragma: no cover

    def _map_error(self, response: Any) -> LinkError:
        if error_v2(response):
            group = int(response.err.group)
            rc = int(response.err.rc)
            if group == 8:  # FILE_MANAGEMENT
                if rc == FS_ERR_FILE_NOT_FOUND:
                    return FileNotFoundOnDevice(f"{self._address}: not found")
                if rc == FS_ERR_FILE_IS_DIRECTORY:
                    return FileIsDirectory(f"{self._address}: is a directory")
            return LinkError(f"{self._address}: SMP error group={group} rc={rc}")
        if error_v1(response):
            rc = int(response.rc)
            if rc == 5:  # MGMT_ERR.ENOENT
                return FileNotFoundOnDevice(f"{self._address}: not found (v1)")
            return LinkError(f"{self._address}: SMP v1 error rc={rc}")
        return LinkError(f"{self._address}: unknown error {response!r}")  # pragma: no cover


async def connect_with_retry(
    link: TempoDeviceLink, *, attempts: int = 3, backoff_s: float = 2.0
) -> None:
    """Connect, retrying discovery misses (device re-advertises after ~1 s).

    Observed live (2026-07-08): back-to-back connects can miss discovery
    because advertising resumes via the firmware's ``recycled`` callback
    shortly after a disconnect.
    """
    last: ConnectError | None = None
    for attempt in range(attempts):
        try:
            await link.connect()
            return
        except ConnectError as exc:
            last = exc
            if attempt < attempts - 1:
                await asyncio.sleep(backoff_s)
    assert last is not None
    raise last
