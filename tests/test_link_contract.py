"""Step 5/6: the TempoDeviceLink behavior contract.

One behavior spec, two implementations: every test in LinkContract runs
against the fake link (offline, always) and — via the `live` marker fixture
added in step 6 — against a real Tempo-BT over BLE. Fake-only fault-injection
behavior lives in TestFakeFaults below.
"""

import hashlib
import io
from collections.abc import AsyncIterator

import pytest

from tempo_tb_ingest.device.fake_link import FakeLink
from tempo_tb_ingest.device.protocol import (
    FileIsDirectory,
    FileNotFoundOnDevice,
    LinkDisconnected,
    SessionListResult,
    TempoDeviceLink,
    log_path,
)

FLIGHT_A = b'$PVER,"Tempo V2 1.5.0",114*72\r\n' + b"$PIMU,1086,-1.05,9.91,-4.13*29\r\n" * 400
FLIGHT_B = b'$PVER,"Tempo V2 1.5.0",114*72\r\n' + b"$PENV,1090,650,233.0*11\r\n" * 90

SESSIONS = {
    "20260705/1CDD8C18": FLIGHT_A,
    "20260705/00BAF6AB": FLIGHT_B,
    "20260201/02E1741B": FLIGHT_B + FLIGHT_A,
}


def make_fake(**kwargs: object) -> FakeLink:
    defaults: dict[str, object] = {"sessions": SESSIONS, "directories": {"/SD:/logs"}}
    defaults.update(kwargs)
    return FakeLink(**defaults)  # type: ignore[arg-type]


@pytest.fixture
async def link() -> AsyncIterator[TempoDeviceLink]:
    """The link under contract test. Step 6 parametrizes this with smp_link."""
    fake = make_fake()
    await fake.connect()
    yield fake
    await fake.disconnect()


class TestLinkContract:
    async def test_session_list_shape(self, link: TempoDeviceLink) -> None:
        result = await link.session_list()
        assert isinstance(result, SessionListResult)
        assert set(result.keys) == set(SESSIONS)
        assert result.truncated is False

    async def test_read_size_of_known_file(self, link: TempoDeviceLink) -> None:
        size = await link.read_size(log_path("20260705/1CDD8C18"))
        assert size == len(FLIGHT_A)

    async def test_read_size_missing_path(self, link: TempoDeviceLink) -> None:
        with pytest.raises(FileNotFoundOnDevice):
            await link.read_size("/SD:/logs/19990101/DEADBEEF/flight.txt")

    async def test_read_size_directory(self, link: TempoDeviceLink) -> None:
        with pytest.raises(FileIsDirectory):
            await link.read_size("/SD:/logs")

    async def test_full_download_is_byte_perfect(self, link: TempoDeviceLink) -> None:
        sink = io.BytesIO()
        written = await link.download(log_path("20260705/1CDD8C18"), sink)
        assert written == len(FLIGHT_A)
        assert hashlib.sha256(sink.getvalue()).digest() == hashlib.sha256(FLIGHT_A).digest()

    async def test_resume_concatenates_to_identical_content(self, link: TempoDeviceLink) -> None:
        path = log_path("20260705/00BAF6AB")
        head = io.BytesIO()
        await link.download(path, head)
        split = len(FLIGHT_B) // 3
        tail = io.BytesIO()
        written = await link.download(path, tail, offset=split)
        assert written == len(FLIGHT_B) - split
        assert head.getvalue()[:split] + tail.getvalue() == FLIGHT_B

    async def test_download_missing_path(self, link: TempoDeviceLink) -> None:
        with pytest.raises(FileNotFoundOnDevice):
            await link.download("/SD:/logs/19990101/DEADBEEF/flight.txt", io.BytesIO())

    async def test_progress_monotonic_and_complete(self, link: TempoDeviceLink) -> None:
        seen: list[int] = []
        await link.download(log_path("20260705/1CDD8C18"), io.BytesIO(), progress=seen.append)
        assert seen == sorted(seen)
        assert seen[-1] == len(FLIGHT_A)

    async def test_probe_testok_absent(self, link: TempoDeviceLink) -> None:
        assert await link.probe_testok() is False

    async def test_never_writes(self, link: TempoDeviceLink) -> None:
        """The read-only guarantee: the interface exposes no write, and none occur."""
        await link.session_list()
        await link.download(log_path("20260705/1CDD8C18"), io.BytesIO())
        forbidden = ("write", "upload", "delete", "logger", "settings_set", "led")
        for call in link.call_log.calls:
            assert not any(word in call.lower() for word in forbidden), call


class TestFakeFaults:
    """Fake-only: the fault-injection hooks that model step-7 hardware behavior."""

    async def test_connect_failures_then_success(self) -> None:
        fake = make_fake(connect_failures=2)
        from tempo_tb_ingest.device.protocol import ConnectError

        for _ in range(2):
            with pytest.raises(ConnectError):
                await fake.connect()
        await fake.connect()
        assert fake.connect_attempts == 3

    async def test_drop_mid_download_keeps_partial_bytes(self) -> None:
        fake = make_fake(drop_at={log_path("20260705/1CDD8C18"): 2048})
        await fake.connect()
        sink = io.BytesIO()
        with pytest.raises(LinkDisconnected):
            await fake.download(log_path("20260705/1CDD8C18"), sink)
        partial = sink.getvalue()
        assert len(partial) == 2048
        assert partial == FLIGHT_A[:2048]

        # reconnect and resume from the partial offset -> identical content
        await fake.connect()
        await fake.download(log_path("20260705/1CDD8C18"), sink, offset=len(partial))
        assert sink.getvalue() == FLIGHT_A

    async def test_drop_is_one_shot(self) -> None:
        fake = make_fake(drop_at={log_path("20260705/00BAF6AB"): 10})
        await fake.connect()
        with pytest.raises(LinkDisconnected):
            await fake.download(log_path("20260705/00BAF6AB"), io.BytesIO())
        await fake.connect()
        sink = io.BytesIO()
        await fake.download(log_path("20260705/00BAF6AB"), sink)
        assert sink.getvalue() == FLIGHT_B

    async def test_operations_require_connection(self) -> None:
        fake = make_fake()
        with pytest.raises(LinkDisconnected):
            await fake.session_list()

    async def test_truncated_flag(self) -> None:
        fake = make_fake(truncated=True)
        await fake.connect()
        assert (await fake.session_list()).truncated is True

    async def test_testok_marker(self) -> None:
        fake = make_fake()
        fake.mark_testok()
        await fake.connect()
        assert await fake.probe_testok() is True

    async def test_testok_directory_counts_as_marked(self) -> None:
        fake = make_fake(directories={"/SD:/logs", "/SD:/testok"})
        await fake.connect()
        assert await fake.probe_testok() is True

    async def test_new_session_appears_between_lists(self) -> None:
        fake = make_fake()
        await fake.connect()
        before = await fake.session_list()
        fake.add_session("20260708/9A8B7C6D", FLIGHT_B)
        after = await fake.session_list()
        assert set(after.keys) - set(before.keys) == {"20260708/9A8B7C6D"}
