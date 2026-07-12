"""SMP custom group 64 ("tempo") message definitions.

Ported from tempo-insights/smpmgr-extensions/plugins/tempo_group.py (which
remains the manual diagnostic harness); firmware side is
tempo-bt/zephyr/tempo-bt-v1/src/mcumgr_custom.c. Only the read-only commands
the daemon uses are ported — harvesting never writes to a device (CLAUDE.md).

SESSION_LIST semantics (firmware v1.5.0): session == jump; each entry is
``{"name": "<YYYYMMDD>/<8HEX>"}``, a session key relative to the device logs
root. The response carries ``truncated`` (bounds: 64 date dirs / 64 sessions
per date). ``truncated`` is optional for tolerance of older firmware.

Firmware v1.6.0 adds paging: the request takes an optional ``page`` (0-based,
64 sessions per page) and the response reports ``page``/``total_pages``/
``total_sessions``. Entries are sorted by date directory descending, then by
session id ascending. All paging fields are optional for tolerance of older
firmware, which ignores ``page`` and returns a single unsorted page.
"""

from enum import IntEnum, unique
from typing import Any

import smp.error as smperr
import smp.message as smpmsg

from tempo_tb_ingest.device.protocol import SessionListResult
from tempo_tb_ingest.device.protocol import StorageInfo as StorageInfoResult

MGMT_GROUP_ID_TEMPO = 64

TEMPO_MGMT_ID_SESSION_LIST = 0
TEMPO_MGMT_ID_STORAGE_INFO = 2


@unique
class TEMPO_RET_RC(IntEnum):
    """Tempo-specific group return codes (mcumgr_custom.c)."""

    OK = 0
    ERROR = 1
    INVALID_STATE = 2
    INVALID_PARAM = 3


class TempoErrorV1(smperr.ErrorV1):
    _GROUP_ID = MGMT_GROUP_ID_TEMPO


class TempoErrorV2(smperr.ErrorV2[TEMPO_RET_RC]):
    _GROUP_ID = MGMT_GROUP_ID_TEMPO


class SessionListRequest(smpmsg.ReadRequest):
    _GROUP_ID = MGMT_GROUP_ID_TEMPO
    _COMMAND_ID = TEMPO_MGMT_ID_SESSION_LIST

    page: int | None = None


class SessionListResponse(smpmsg.ReadResponse):
    _GROUP_ID = MGMT_GROUP_ID_TEMPO
    _COMMAND_ID = TEMPO_MGMT_ID_SESSION_LIST

    sessions: list[dict[str, Any]]
    count: int
    truncated: bool | None = None
    page: int | None = None
    total_pages: int | None = None
    total_sessions: int | None = None


class SessionList(SessionListRequest):
    _Response = SessionListResponse
    _ErrorV1 = TempoErrorV1
    _ErrorV2 = TempoErrorV2


class StorageInfoRequest(smpmsg.ReadRequest):
    _GROUP_ID = MGMT_GROUP_ID_TEMPO
    _COMMAND_ID = TEMPO_MGMT_ID_STORAGE_INFO


class StorageInfoResponse(smpmsg.ReadResponse):
    _GROUP_ID = MGMT_GROUP_ID_TEMPO
    _COMMAND_ID = TEMPO_MGMT_ID_STORAGE_INFO

    backend: str
    free_bytes: int
    total_bytes: int
    used_percent: int


class StorageInfo(StorageInfoRequest):
    _Response = StorageInfoResponse
    _ErrorV1 = TempoErrorV1
    _ErrorV2 = TempoErrorV2


def session_list_result(response: SessionListResponse) -> SessionListResult:
    """Convert the wire response to the link-interface result type."""
    keys = [str(entry["name"]) for entry in response.sessions if "name" in entry]
    return SessionListResult(keys=keys, truncated=bool(response.truncated))


def storage_info_result(response: StorageInfoResponse) -> StorageInfoResult:
    return StorageInfoResult(
        backend=response.backend,
        free_bytes=response.free_bytes,
        total_bytes=response.total_bytes,
        used_percent=response.used_percent,
    )
