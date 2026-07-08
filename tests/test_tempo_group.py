"""Step 5: group-64 message codecs (CBOR round-trips, wire constants)."""

import pytest

from tempo_tb_ingest.device import tempo_group as tg
from tempo_tb_ingest.device.protocol import SessionListResult


class TestWireConstants:
    """These constants are the firmware ABI (mcumgr_custom.c) — locked."""

    def test_group_id(self) -> None:
        assert tg.MGMT_GROUP_ID_TEMPO == 64

    def test_command_ids(self) -> None:
        assert tg.TEMPO_MGMT_ID_SESSION_LIST == 0
        assert tg.TEMPO_MGMT_ID_STORAGE_INFO == 2

    def test_session_list_request_header(self) -> None:
        req = tg.SessionList()
        assert req.header.group_id == 64
        assert req.header.command_id == 0
        assert req.header.op.name == "READ"

    def test_storage_info_request_header(self) -> None:
        req = tg.StorageInfo()
        assert req.header.group_id == 64
        assert req.header.command_id == 2
        assert req.header.op.name == "READ"


class TestSessionListCodec:
    def test_response_round_trips_through_cbor(self) -> None:
        resp = tg.SessionListResponse(
            sequence=0,
            sessions=[{"name": "20260705/1CDD8C18"}, {"name": "20260705/00BAF6AB"}],
            count=2,
            truncated=False,
        )
        parsed = tg.SessionListResponse.loads(resp.BYTES)
        assert parsed.sessions == resp.sessions
        assert parsed.count == 2
        assert parsed.truncated is False

    def test_truncated_optional_for_old_firmware(self) -> None:
        resp = tg.SessionListResponse(sequence=0, sessions=[], count=0)
        parsed = tg.SessionListResponse.loads(resp.BYTES)
        assert parsed.truncated is None

    def test_result_conversion(self) -> None:
        resp = tg.SessionListResponse(
            sequence=0,
            sessions=[{"name": "20260705/1CDD8C18"}],
            count=1,
            truncated=True,
        )
        result = tg.session_list_result(resp)
        assert result == SessionListResult(keys=["20260705/1CDD8C18"], truncated=True)

    def test_result_conversion_treats_missing_truncated_as_false(self) -> None:
        resp = tg.SessionListResponse(sequence=0, sessions=[], count=0)
        assert tg.session_list_result(resp).truncated is False

    def test_malformed_keys_from_device_are_rejected(self) -> None:
        resp = tg.SessionListResponse(
            sequence=0,
            sessions=[{"name": "not-a-session-key"}],
            count=1,
            truncated=False,
        )
        with pytest.raises(ValueError, match="malformed session keys"):
            tg.session_list_result(resp)


class TestStorageInfoCodec:
    def test_round_trip_and_conversion(self) -> None:
        resp = tg.StorageInfoResponse(
            sequence=0,
            backend="sdcard",
            free_bytes=31_048_826_880,
            total_bytes=31_086_084_096,
            used_percent=1,
        )
        parsed = tg.StorageInfoResponse.loads(resp.BYTES)
        info = tg.storage_info_result(parsed)
        assert info.backend == "sdcard"
        assert info.total_bytes == 31_086_084_096
        assert info.used_percent == 1
