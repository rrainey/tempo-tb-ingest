"""Real TempoDeviceLink over SMP/BLE via smpclient → bleak → BlueZ (design §3.1).

Implementation notes:

- The chunked download loop is driven here (not ``SMPClient.download_file``)
  because harvest needs offset resume, progress callbacks, and **pipelining**:
  measured 2026-07-10, keeping two fs-download requests in flight lifts the
  dongle path from 26.7 to 75.9 KB/s (the serial request/response cadence,
  quantized to BLE connection intervals, was the throughput ceiling — not the
  radio and not HCI/USB). Window 2 is the default; window ≥4 exhausted the
  device's SMP netbuf pool in testing (lost responses).
- Any pipelining anomaly (response timeout, unknown sequence, off mismatch,
  short chunk) drains the transport and falls back to the proven serial loop,
  resuming from the contiguous prefix already written. Real SMP errors
  (file-not-found) and disconnects are raised, never retried silently.
- The fs download response carries the file's total length only when
  ``off == 0``, so a resumed download asks ``FileStatus`` for the total first.
- ``address`` may be a BLE name ("Tempo-BT-0001") or a MAC; the harvest
  worker passes the most recently sighted MAC (design §3.3) and falls back to
  the name.
- Adapter selection for multi-dongle pools (radio Option 2) is not yet
  plumbed through smpclient's BLE transport; tracked in plan step 21.
- Exception mapping to the LinkError taxonomy is centralized here; step 7
  (fault characterization) refines it against real radio drops.
"""

import asyncio
import contextlib
import logging
from typing import IO, Any

from pydantic import ValidationError
from smp import header as smpheader
from smp.file_management import (
    FileDownloadResponse,
    FileSystemManagementErrorV1,
    FileSystemManagementErrorV2,
)
from smpclient import SMPClient
from smpclient.generics import error, error_v1, error_v2, success
from smpclient.requests.file_management import FileDownload, FileStatus
from smpclient.transport import SMPTransport, SMPTransportDisconnected
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

logger = logging.getLogger(__name__)


class PipelinedSMPBLETransport(SMPBLETransport):
    """SMPBLETransport with coalescing-safe receive() and adapter binding.

    receive(): with pipelined requests, two responses can land in the notify
    buffer back-to-back; the stock implementation raises on
    ``len(buffer) > message`` and clears the whole buffer on return (observed
    live 2026-07-10). This subclass slices exactly one message off the front
    and preserves the rest.

    adapter binding (design §3.13): BlueZ can only connect a device that the
    *same controller* has discovered, so target discovery and the client are
    both bound to ``adapter`` ("hciN"); None = system default. The stock
    transport offers no adapter selection — worth an upstream PR.
    """

    def __init__(self, adapter: str | None = None) -> None:
        super().__init__()
        self._adapter = adapter

    async def _connect(self, address: str, timeout_s: float) -> None:
        if self._adapter is None:
            await super()._connect(address, timeout_s)
            return
        # mirror of the stock _connect with the adapter kwarg threaded through
        from bleak import BleakClient, BleakScanner
        from bleak.backends.bluezdbus.scanner import BlueZScannerArgs
        from smpclient.transport import (
            SMP_CHARACTERISTIC_UUID,
            SMP_SERVICE_UUID,
        )
        from smpclient.transport.ble import (
            MAC_ADDRESS_PATTERN,
            UUID_PATTERN,
            SMPBLETransportDeviceNotFound,
            SMPBLETransportNotSMPServer,
        )

        by_address = bool(MAC_ADDRESS_PATTERN.match(address) or UUID_PATTERN.match(address))
        bluez_args = BlueZScannerArgs(adapter=self._adapter)  # non-deprecated kwarg form
        device = await (
            BleakScanner.find_device_by_address(address, timeout=timeout_s, bluez=bluez_args)
            if by_address
            else BleakScanner.find_device_by_name(address, timeout=timeout_s, bluez=bluez_args)
        )
        if device is None:
            raise SMPBLETransportDeviceNotFound(f"Device '{address}' not found on {self._adapter}")
        self._client = BleakClient(
            device,
            services=(str(SMP_SERVICE_UUID),),
            timeout=timeout_s,
            disconnected_callback=self._set_disconnected_event,
        )
        await self._client.connect()
        self._disconnected_event.clear()

        smp_characteristic = self._client.services.get_characteristic(SMP_CHARACTERISTIC_UUID)
        if smp_characteristic is None:
            raise SMPBLETransportNotSMPServer("Missing the SMP characteristic UUID.")
        self._max_write_without_response_size = smp_characteristic.max_write_without_response_size
        if self._bluez_backend(self._client._backend):
            await self._client._backend._acquire_mtu()
            self._max_write_without_response_size = self._client.mtu_size - 3
        logger.info("adapter=%s %s", self._adapter, f"{self._max_write_without_response_size=}")
        self._smp_characteristic = smp_characteristic
        await self._await_or_disconnect(
            self._client.start_notify(
                SMP_CHARACTERISTIC_UUID,
                self._notify_callback,  # type: ignore[arg-type]  # smpclient's own signature
            )
        )

    async def receive(self) -> bytes:
        async with self._notify_condition:
            while len(self._buffer) < smpheader.Header.SIZE:
                await self._notify_or_disconnect()
            header = smpheader.Header.loads(bytes(self._buffer[: smpheader.Header.SIZE]))
            message_length = header.length + header.SIZE
            while len(self._buffer) < message_length:
                await self._notify_or_disconnect()
            out = bytes(self._buffer[:message_length])
            del self._buffer[:message_length]
            return out


FS_ERR_FILE_NOT_FOUND = 3  # FS_MGMT_ERR.FILE_NOT_FOUND (verified live 2026-07-08)
FS_ERR_FILE_IS_DIRECTORY = 4  # FS_MGMT_ERR.FILE_IS_DIRECTORY

#: fs-download requests kept in flight (measured optimum; ≥4 overruns the
#: device's SMP netbuf pool). 0 or 1 disables pipelining.
DEFAULT_PIPELINE_WINDOW = 2


class _PipelineAnomaly(Exception):
    """Internal: pipelined download hit something odd; fall back to serial."""

    def __init__(self, written: int, reason: str, total: int | None = None) -> None:
        super().__init__(reason)
        self.written = written  # contiguous bytes already delivered to the sink
        self.total = total  # file length if already learned


class SmpLink(TempoDeviceLink):
    """One BLE connection to a Tempo-BT device."""

    def __init__(
        self,
        address: str,
        *,
        connect_timeout_s: float = 20.0,
        request_timeout_s: float = 10.0,
        pipeline_window: int = DEFAULT_PIPELINE_WINDOW,
        adapter: str | None = None,
        client: SMPClient | None = None,
    ) -> None:
        self.call_log = LinkCallLog()
        self._address = address
        self._connect_timeout_s = connect_timeout_s
        self._request_timeout_s = request_timeout_s
        self._pipeline_window = pipeline_window
        self.adapter = adapter  # "hciN" or None = system default
        self.pipeline_fallbacks = 0  # observability: anomalies that degraded to serial
        self._client = client or SMPClient(
            PipelinedSMPBLETransport(adapter=adapter), address, timeout_s=request_timeout_s
        )

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

        if self._pipeline_window >= 2:
            try:
                return await self._download_pipelined(
                    path, sink, offset=offset, total=total, progress=progress
                )
            except _PipelineAnomaly as anomaly:
                self.pipeline_fallbacks += 1
                logger.warning(
                    "%s: pipelined download anomaly at +%d bytes (%s); "
                    "draining and falling back to serial",
                    self._address,
                    anomaly.written,
                    anomaly,
                )
                await self._drain_transport()
                known_total = anomaly.total if anomaly.total is not None else total
                if known_total is not None and offset + anomaly.written >= known_total:
                    return anomaly.written
                resumed = await self._download_serial(
                    path,
                    sink,
                    offset=offset + anomaly.written,
                    progress=progress,
                    total=known_total,
                )
                return anomaly.written + resumed
        return await self._download_serial(
            path, sink, offset=offset, progress=progress, total=total
        )

    async def _download_serial(
        self,
        path: str,
        sink: IO[bytes],
        *,
        offset: int,
        progress: ProgressFn | None,
        total: int | None = None,
    ) -> int:
        """The proven one-request-at-a-time loop (fallback + window<2 mode)."""
        if offset > 0 and total is None:
            total = await self.read_size(path)
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

    async def _download_pipelined(
        self,
        path: str,
        sink: IO[bytes],
        *,
        offset: int,
        total: int | None,
        progress: ProgressFn | None,
    ) -> int:
        """Windowed fs download (2026-07-10): hides per-chunk round-trip latency.

        The sink only ever receives the contiguous prefix, so an anomaly can
        always fall back to the serial loop at ``offset + written``.
        """
        transport = self._transport()
        written = 0

        # first chunk serially: learns total (off==0) and the chunk size
        first = await self._request(FileDownload(off=offset, name=path))
        if offset == 0:
            if first.len is None:
                raise LinkError(f"device never reported a length for {path}")
            total = int(first.len)
        assert total is not None
        data = bytes(first.data)
        if not data and offset < total:
            raise LinkError(f"device returned empty chunk at offset {offset} of {path}")
        sink.write(data)
        written += len(data)
        if progress is not None:
            progress(offset + written)
        chunk_size = len(data)
        if offset + written >= total:
            return written

        offsets = list(range(offset + written, total, chunk_size))
        pending: dict[int, int] = {}  # smp sequence -> requested offset
        buffered: dict[int, bytes] = {}  # offset -> chunk (not yet contiguous)
        next_index = 0

        async def send_next() -> None:
            nonlocal next_index
            if next_index < len(offsets):
                request = FileDownload(off=offsets[next_index], name=path)
                pending[request.header.sequence] = offsets[next_index]
                next_index += 1
                await transport.send(request.BYTES)

        for _ in range(min(self._pipeline_window, len(offsets))):
            await send_next()

        while pending:
            try:
                frame = await asyncio.wait_for(
                    transport.receive(), timeout=self._request_timeout_s
                )
            except TimeoutError as exc:
                raise _PipelineAnomaly(written, f"response timeout: {exc}", total) from exc
            except SMPTransportDisconnected as exc:
                raise LinkDisconnected(f"{self._address}: {exc}") from exc

            header = smpheader.Header.loads(frame[: smpheader.Header.SIZE])
            if header.sequence not in pending:
                raise _PipelineAnomaly(written, f"unexpected sequence {header.sequence}", total)
            requested_off = pending.pop(header.sequence)

            try:
                response = FileDownloadResponse.loads(frame)
            except ValidationError:
                raise self._parse_fs_error(frame, written, total) from None

            if int(response.off) != requested_off:
                raise _PipelineAnomaly(
                    written, f"off mismatch: asked {requested_off}, got {response.off}", total
                )
            data = bytes(response.data)
            is_final = requested_off + len(data) >= total
            if len(data) != chunk_size and not is_final:
                raise _PipelineAnomaly(
                    written, f"short chunk ({len(data)} B) at offset {requested_off}", total
                )
            if not data:
                raise _PipelineAnomaly(written, f"empty chunk at offset {requested_off}", total)
            buffered[requested_off] = data

            while (offset + written) in buffered:
                piece = buffered.pop(offset + written)
                sink.write(piece)
                written += len(piece)
                if progress is not None:
                    progress(offset + written)
            await send_next()

        if offset + written != total:
            raise _PipelineAnomaly(
                written, f"finished with {offset + written} != total {total}", total
            )
        return written

    def _parse_fs_error(self, frame: bytes, written: int, total: int | None) -> Exception:
        """A non-FileDownloadResponse frame: a real SMP error, or an anomaly."""
        for error_cls in (FileSystemManagementErrorV2, FileSystemManagementErrorV1):
            try:
                return self._map_error(error_cls.loads(frame))
            except ValidationError:
                continue
        return _PipelineAnomaly(written, "unparseable response frame", total)

    def _transport(self) -> SMPTransport:
        return self._client._transport

    async def _drain_transport(self) -> None:
        """Discard stale in-flight responses before serial resume — a stale
        frame would otherwise break SMPClient.request()'s sequence check."""
        with contextlib.suppress(Exception):
            while True:
                await asyncio.wait_for(self._transport().receive(), timeout=1.0)

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
