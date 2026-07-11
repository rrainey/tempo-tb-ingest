"""Steps 5/6: the TempoDeviceLink behavior contract.

One behavior spec, two implementations. Every test in TestLinkContract runs
against the fake link (offline, always) and — under ``-m live`` with a
Tempo-BT in range — against the real smp_link over BLE. Each implementation
supplies a ``DeviceTruth`` describing what is known to be on the device, so
the assertions are data-agnostic. Fake-only fault-injection behavior lives in
TestFakeFaults.

Live tier: read-only throughout; expects device ``TEMPO_LIVE_DEVICE``
(default Tempo-BT-0001) whose session ``20260201/02E1741B`` is already staged
in tempo-testbed/device-data (the byte-identity reference).
"""

import hashlib
import io
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from tempo_tb_ingest.device.fake_link import FakeLink
from tempo_tb_ingest.device.protocol import (
    ConnectError,
    FileIsDirectory,
    FileNotFoundOnDevice,
    LinkDisconnected,
    SessionListResult,
    TempoDeviceLink,
    log_path,
)

# --------------------------------------------------------------------------- #
# truths

FLIGHT_A = b'$PVER,"Tempo V2 1.5.0",114*72\r\n' + b"$PIMU,1086,-1.05,9.91,-4.13*29\r\n" * 400
FLIGHT_B = b'$PVER,"Tempo V2 1.5.0",114*72\r\n' + b"$PENV,1090,650,233.0*11\r\n" * 90

FAKE_SESSIONS = {
    "20260705/1CDD8C18": FLIGHT_A,
    "20260705/00BAF6AB": FLIGHT_B,
    "20260201/02E1741B": FLIGHT_B + FLIGHT_A,
}

LIVE_DEVICE = os.environ.get("TEMPO_LIVE_DEVICE", "Tempo-BT-0001")
LIVE_REFERENCE = Path(
    os.environ.get(
        "TEMPO_LIVE_REFERENCE",
        "/home/riley/src/tempo-testbed/device-data/TempoBT-0001/logs/20260201/02E1741B/flight.txt",
    )
)
LIVE_SESSION = "20260201/02E1741B"


@dataclass
class DeviceTruth:
    """What the contract may assume exists on the device under test."""

    expected_sessions: set[str]  # subset of what session_list must report
    known_session: str  # session whose flight.txt content is known
    known_size: int
    known_sha256: str
    missing_path: str
    dir_path: str
    testok_expected: bool


def fake_truth() -> DeviceTruth:
    content = FAKE_SESSIONS["20260705/1CDD8C18"]
    return DeviceTruth(
        expected_sessions=set(FAKE_SESSIONS),
        known_session="20260705/1CDD8C18",
        known_size=len(content),
        known_sha256=hashlib.sha256(content).hexdigest(),
        missing_path="/SD:/logs/19990101/DEADBEEF/flight.txt",
        dir_path="/SD:/logs",
        testok_expected=False,
    )


def live_truth() -> DeviceTruth:
    if not LIVE_REFERENCE.is_file():
        pytest.skip(f"live reference copy not found: {LIVE_REFERENCE}")
    content = LIVE_REFERENCE.read_bytes()
    return DeviceTruth(
        expected_sessions={LIVE_SESSION},
        known_session=LIVE_SESSION,
        known_size=len(content),
        known_sha256=hashlib.sha256(content).hexdigest(),
        missing_path="/SD:/logs/19990101/DEADBEEF/flight.txt",
        dir_path=f"/SD:/logs/{LIVE_SESSION.split('/')[0]}",
        testok_expected=os.environ.get("TEMPO_LIVE_TESTOK", "") == "1",
    )


def make_fake(**kwargs: object) -> FakeLink:
    defaults: dict[str, object] = {"sessions": FAKE_SESSIONS, "directories": {"/SD:/logs"}}
    defaults.update(kwargs)
    return FakeLink(**defaults)  # type: ignore[arg-type]


@dataclass
class Rig:
    link: TempoDeviceLink
    truth: DeviceTruth


@pytest.fixture(
    params=["fake", pytest.param("live", marks=pytest.mark.live)],
)
async def rig(request: pytest.FixtureRequest) -> AsyncIterator[Rig]:
    if request.param == "fake":
        link: TempoDeviceLink = make_fake()
        truth = fake_truth()
        await link.connect()
    else:
        from tempo_tb_ingest.device.smp_link import SmpLink, connect_with_retry

        truth = live_truth()
        # TEMPO_ADAPTER binds the live tier to a specific controller ("hciN");
        # unset = system default (step 21: run the same suite via a dongle)
        link = SmpLink(
            LIVE_DEVICE,
            connect_timeout_s=15.0,
            adapter=os.environ.get("TEMPO_ADAPTER"),
        )
        await connect_with_retry(link, attempts=4, backoff_s=3.0)
    yield Rig(link=link, truth=truth)
    await link.disconnect()


# --------------------------------------------------------------------------- #
# the contract


class TestLinkContract:
    async def test_session_list_shape(self, rig: Rig) -> None:
        result = await rig.link.session_list()
        assert isinstance(result, SessionListResult)
        assert rig.truth.expected_sessions <= set(result.keys)
        assert isinstance(result.truncated, bool)

    async def test_read_size_of_known_file(self, rig: Rig) -> None:
        size = await rig.link.read_size(log_path(rig.truth.known_session))
        assert size == rig.truth.known_size

    async def test_read_size_missing_path(self, rig: Rig) -> None:
        with pytest.raises(FileNotFoundOnDevice):
            await rig.link.read_size(rig.truth.missing_path)

    async def test_read_size_directory(self, rig: Rig) -> None:
        with pytest.raises(FileIsDirectory):
            await rig.link.read_size(rig.truth.dir_path)

    async def test_full_download_is_byte_perfect(self, rig: Rig) -> None:
        sink = io.BytesIO()
        written = await rig.link.download(log_path(rig.truth.known_session), sink)
        assert written == rig.truth.known_size
        assert hashlib.sha256(sink.getvalue()).hexdigest() == rig.truth.known_sha256

    async def test_resume_concatenates_to_identical_content(self, rig: Rig) -> None:
        path = log_path(rig.truth.known_session)
        split = rig.truth.known_size // 3
        head = io.BytesIO()
        await rig.link.download(path, head)
        tail = io.BytesIO()
        written = await rig.link.download(path, tail, offset=split)
        assert written == rig.truth.known_size - split
        whole = head.getvalue()[:split] + tail.getvalue()
        assert hashlib.sha256(whole).hexdigest() == rig.truth.known_sha256

    async def test_download_missing_path(self, rig: Rig) -> None:
        with pytest.raises(FileNotFoundOnDevice):
            await rig.link.download(rig.truth.missing_path, io.BytesIO())

    async def test_progress_monotonic_and_complete(self, rig: Rig) -> None:
        seen: list[int] = []
        path = log_path(rig.truth.known_session)
        await rig.link.download(path, io.BytesIO(), progress=seen.append)
        assert seen == sorted(seen)
        assert seen[-1] == rig.truth.known_size

    async def test_probe_testok(self, rig: Rig) -> None:
        assert await rig.link.probe_testok() is rig.truth.testok_expected

    async def test_never_writes(self, rig: Rig) -> None:
        """The read-only guarantee: the interface exposes no write, and none occur."""
        await rig.link.session_list()
        await rig.link.download(log_path(rig.truth.known_session), io.BytesIO())
        forbidden = ("write", "upload", "delete", "logger", "settings_set", "led")
        for call in rig.link.call_log.calls:
            assert not any(word in call.lower() for word in forbidden), call


# --------------------------------------------------------------------------- #
# fake-only fault injection (models step-7 hardware characterization)


class TestFakeFaults:
    async def test_connect_failures_then_success(self) -> None:
        fake = make_fake(connect_failures=2)
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
