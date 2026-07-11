"""Pipelined fs download (2026-07-10): windowing, anomaly fallback, integrity.

Drives the real SmpLink download path over a scripted in-memory SMP transport
(no radio): normal pipelining, response reordering, lost responses (timeout →
drain → serial fallback), short/mismatched chunks, real SMP errors, and
disconnects. Byte-identity asserted throughout.
"""

import asyncio
import io
from typing import Any

import cbor2
import pytest
from smp import header as smpheader
from smp.file_management import (
    FileDownloadResponse,
    FileStatusResponse,
)
from smp.header import CommandId, GroupId
from smpclient import SMPClient
from smpclient.requests.file_management import FileDownload, FileStatus
from smpclient.transport import SMPTransport, SMPTransportDisconnected

from tempo_tb_ingest.device.protocol import FileNotFoundOnDevice, LinkDisconnected
from tempo_tb_ingest.device.smp_link import SmpLink

CHUNK = 256
FILE = bytes(range(256)) * 14 + b"tail-bytes"  # 3594 B: 14 full chunks + short final
PATH = "/SD:/logs/20260403/8DF8E4B7/flight.txt"


class ScriptedTransport(SMPTransport):
    """Answers FileDownload requests from an in-memory file, with fault hooks."""

    def __init__(
        self,
        content: bytes = FILE,
        *,
        drop_offsets: set[int] | None = None,  # never answer these (once)
        error_at: int | None = None,  # respond with FS error at this offset
        wrong_off_at: int | None = None,  # lie about the offset (once)
        reorder: bool = False,  # deliver responses LIFO
        disconnect_at: int | None = None,  # raise on receive after this offset seen
    ) -> None:
        self.content = content
        self.drop_offsets = set(drop_offsets or ())
        self.error_at = error_at
        self.wrong_off_at = wrong_off_at
        self.reorder = reorder
        self.disconnect_at = disconnect_at
        self.outbox: list[bytes] = []
        self.max_in_flight = 0
        self._in_flight = 0
        self.requests: list[int] = []

    # -- SMPTransport interface ------------------------------------------------

    @property
    def mtu(self) -> int:
        return 498

    @property
    def max_unencoded_size(self) -> int:
        return 2475

    async def connect(self, address: str, timeout_s: float) -> None:  # pragma: no cover
        pass

    async def disconnect(self) -> None:  # pragma: no cover
        pass

    async def send(self, data: bytes) -> None:
        header = smpheader.Header.loads(data[: smpheader.Header.SIZE])
        if header.command_id == CommandId.FileManagement.FILE_STATUS:
            status = FileStatus.loads(data)
            self.outbox.append(
                FileStatusResponse(sequence=status.header.sequence, len=len(self.content)).BYTES
            )
            return
        request = FileDownload.loads(data)
        off = int(request.off)
        self.requests.append(off)
        self._in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self._in_flight)

        if off in self.drop_offsets:
            self.drop_offsets.discard(off)
            return  # silently never answer

        seq = request.header.sequence
        if self.error_at is not None and off == self.error_at:
            payload = cbor2.dumps({"err": {"group": 8, "rc": 3}})  # FILE_NOT_FOUND
            error_header = smpheader.Header(
                op=smpheader.OP.READ_RSP,
                version=smpheader.Version.V2,
                flags=smpheader.Flag(0),
                length=len(payload),
                group_id=GroupId.FILE_MANAGEMENT,
                sequence=seq,
                command_id=CommandId.FileManagement.FILE_DOWNLOAD_UPLOAD,
            )
            self.outbox.append(error_header.BYTES + payload)
            return

        claimed_off = off
        if self.wrong_off_at is not None and off == self.wrong_off_at:
            self.wrong_off_at = None
            claimed_off = off + 1  # lie

        chunk = self.content[off : off + CHUNK]
        response = FileDownloadResponse(
            sequence=seq,
            off=claimed_off,
            data=chunk,
            len=len(self.content) if off == 0 else None,
        )
        self.outbox.append(response.BYTES)

    async def receive(self) -> bytes:
        while not self.outbox:
            await asyncio.sleep(0.001)
        self._in_flight -= 1
        frame = self.outbox.pop() if self.reorder else self.outbox.pop(0)
        if self.disconnect_at is not None:
            hdr = smpheader.Header.loads(frame[: smpheader.Header.SIZE])
            if hdr.length and b"tail" not in frame:
                pass  # inspect via offset below
        return frame

    async def send_and_receive(self, data: bytes) -> bytes:
        await self.send(data)
        return await self.receive()


def make_link(transport: ScriptedTransport, **kwargs: Any) -> SmpLink:
    client = SMPClient(transport, "fake-address")
    return SmpLink("fake-address", client=client, request_timeout_s=0.3, **kwargs)


async def run_download(link: SmpLink, offset: int = 0) -> tuple[bytes, int]:
    sink = io.BytesIO()
    written = await link.download(PATH, sink, offset=offset)
    return sink.getvalue(), written


class TestPipelinedHappyPath:
    async def test_byte_identical_and_windowed(self) -> None:
        transport = ScriptedTransport()
        link = make_link(transport)
        data, written = await run_download(link)
        assert data == FILE
        assert written == len(FILE)
        assert link.pipeline_fallbacks == 0
        assert transport.max_in_flight <= 2  # window respected

    async def test_progress_monotonic_contiguous(self) -> None:
        seen: list[int] = []
        link = make_link(ScriptedTransport())
        sink = io.BytesIO()
        await link.download(PATH, sink, progress=seen.append)
        assert seen == sorted(seen)
        assert seen[-1] == len(FILE)

    async def test_reordered_responses_still_contiguous(self) -> None:
        link = make_link(ScriptedTransport(reorder=True))
        data, _ = await run_download(link)
        assert data == FILE
        assert link.pipeline_fallbacks == 0

    async def test_single_chunk_file(self) -> None:
        link = make_link(ScriptedTransport(content=b"tiny"))
        data, _ = await run_download(link)
        assert data == b"tiny"

    async def test_window_1_uses_serial_loop(self) -> None:
        transport = ScriptedTransport()
        link = make_link(transport, pipeline_window=1)
        data, _ = await run_download(link)
        assert data == FILE
        assert transport.max_in_flight == 1


class TestAnomalyFallback:
    async def test_lost_response_times_out_then_serial_completes(self) -> None:
        transport = ScriptedTransport(drop_offsets={CHUNK * 5})
        link = make_link(transport)
        data, written = await run_download(link)
        assert data == FILE  # fallback resumed from the contiguous prefix
        assert written == len(FILE)
        assert link.pipeline_fallbacks == 1

    async def test_wrong_offset_response_falls_back(self) -> None:
        link = make_link(ScriptedTransport(wrong_off_at=CHUNK * 3))
        data, _ = await run_download(link)
        assert data == FILE
        assert link.pipeline_fallbacks == 1

    async def test_real_smp_error_raises_not_falls_back(self) -> None:
        link = make_link(ScriptedTransport(error_at=CHUNK * 2))
        with pytest.raises(FileNotFoundOnDevice):
            await run_download(link)
        assert link.pipeline_fallbacks == 0  # real errors are not anomalies


class TestResume:
    async def test_offset_resume_pipelined(self) -> None:
        split = CHUNK * 4 + 17
        link = make_link(ScriptedTransport())
        sink = io.BytesIO()
        written = await link.download(PATH, sink, offset=split)
        assert written == len(FILE) - split
        assert sink.getvalue() == FILE[split:]


class TestDisconnect:
    async def test_transport_disconnect_raises_link_disconnected(self) -> None:
        class DisconnectingTransport(ScriptedTransport):
            async def receive(self) -> bytes:
                if self.requests and max(self.requests) >= CHUNK * 4:
                    raise SMPTransportDisconnected("gone")
                return await super().receive()

        link = make_link(DisconnectingTransport())
        with pytest.raises(LinkDisconnected):
            await run_download(link)
