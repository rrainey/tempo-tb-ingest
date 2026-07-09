"""Configuration: TOML file + TEMPO_INGEST_* environment overrides.

Surface documented in docs/design.md §3.9. Precedence (highest wins):
environment > TOML file > defaults.

Environment override naming: ``TEMPO_INGEST_<SECTION>__<FIELD>`` (double
underscore between section and field), e.g. ``TEMPO_INGEST_DETECTION__ABSENT_AFTER_S=30``.
Values are strings, coerced by the field type; list fields (adapter.transfer)
split on commas.
"""

import os
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator

DEFAULT_CONFIG_PATH = Path("/etc/tempo-tb-ingest.toml")
ENV_PREFIX = "TEMPO_INGEST_"


class ConfigError(Exception):
    """Invalid or unreadable configuration."""


class _Section(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AdapterConfig(_Section):
    scan: str = "hci0"
    transfer: list[str] = Field(default_factory=lambda: ["hci0"])

    @field_validator("scan")
    @classmethod
    def _scan_nonempty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("adapter.scan must be a non-empty adapter name")
        return v

    @field_validator("transfer")
    @classmethod
    def _transfer_nonempty(cls, v: list[str]) -> list[str]:
        if not v or any(not a.strip() for a in v):
            raise ValueError("adapter.transfer must be a non-empty list of adapter names")
        return v


class DetectionConfig(_Section):
    rssi_floor_dbm: int = -75
    lost_after_s: float = Field(default=90.0, gt=0)
    absent_after_s: float = Field(default=600.0, gt=0)

    @field_validator("rssi_floor_dbm")
    @classmethod
    def _rssi_plausible(cls, v: int) -> int:
        if not -127 <= v <= 0:
            raise ValueError("detection.rssi_floor_dbm must be in [-127, 0]")
        return v


class HarvestConfig(_Section):
    connect_timeout_s: float = Field(default=20.0, gt=0)
    max_attempts: int = Field(default=5, ge=1)
    spool_dir: Path = Path("/var/lib/tempo-tb-ingest/spool")


class StoreConfig(_Section):
    staging_root: Path = Path("/home/riley/src/tempo-testbed/device-data")
    data_dir: Path = Path("/var/lib/tempo-tb-ingest")
    owners_file: Path | None = None  # default: <staging_root>/device-owners.json

    def resolved_owners_file(self) -> Path:
        return self.owners_file or self.staging_root / "device-owners.json"


class PromoteConfig(_Section):
    test_data_root: Path = Path("/home/riley/src/tempo-testbed/test-data")
    exit_window_s: float = Field(default=120.0, gt=0)
    gps_max_separation_m: float = Field(default=500.0, gt=0)


class DropzoneConfig(_Section):
    """Copied verbatim into generated metadata.json (design §3.11)."""

    name: str = "Texoma (North TX)"
    lat_deg: float = Field(default=33.4569, ge=-90, le=90)
    lon_deg: float = Field(default=-96.3770, ge=-180, le=180)
    elevation_m: float = 233.0
    timezone: str = "America/Chicago"

    @field_validator("name", "timezone")
    @classmethod
    def _nonempty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("dropzone.name and dropzone.timezone must be non-empty")
        return v


class HttpConfig(_Section):
    listen: str = "127.0.0.1:8080"
    static_dir: Path | None = None  # built dashboard (dashboard/dist); None = API only

    @field_validator("listen")
    @classmethod
    def _host_port(cls, v: str) -> str:
        host, sep, port = v.rpartition(":")
        if not sep or not host or not port.isdigit() or not 0 < int(port) < 65536:
            raise ValueError("http.listen must be '<host>:<port>'")
        return v

    @property
    def host(self) -> str:
        return self.listen.rpartition(":")[0]

    @property
    def port(self) -> int:
        return int(self.listen.rpartition(":")[2])


class LogConfig(_Section):
    level: str = "info"

    @field_validator("level")
    @classmethod
    def _known_level(cls, v: str) -> str:
        allowed = {"debug", "info", "warning", "error"}
        if v.lower() not in allowed:
            raise ValueError(f"log.level must be one of {sorted(allowed)}")
        return v.lower()


class Config(_Section):
    adapter: AdapterConfig = Field(default_factory=AdapterConfig)
    detection: DetectionConfig = Field(default_factory=DetectionConfig)
    harvest: HarvestConfig = Field(default_factory=HarvestConfig)
    store: StoreConfig = Field(default_factory=StoreConfig)
    promote: PromoteConfig = Field(default_factory=PromoteConfig)
    dropzone: DropzoneConfig = Field(default_factory=DropzoneConfig)
    http: HttpConfig = Field(default_factory=HttpConfig)
    log: LogConfig = Field(default_factory=LogConfig)

    @classmethod
    def load(
        cls,
        path: Path | None = None,
        env: Mapping[str, str] | None = None,
    ) -> Self:
        """Load configuration.

        ``path=None`` reads DEFAULT_CONFIG_PATH if it exists, else pure defaults.
        An explicitly given path must exist. ``env`` defaults to ``os.environ``.
        """
        data: dict[str, Any] = {}
        if path is None:
            if DEFAULT_CONFIG_PATH.is_file():
                data = _read_toml(DEFAULT_CONFIG_PATH)
        else:
            if not path.is_file():
                raise ConfigError(f"config file not found: {path}")
            data = _read_toml(path)

        _apply_env_overrides(data, os.environ if env is None else env)

        try:
            return cls.model_validate(data)
        except ValueError as exc:
            raise ConfigError(str(exc)) from exc


def _read_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as fh:
            return tomllib.load(fh)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"cannot read {path}: {exc}") from exc


_LIST_FIELDS = {("adapter", "transfer")}


def _apply_env_overrides(data: dict[str, Any], env: Mapping[str, str]) -> None:
    for key, raw in env.items():
        if not key.startswith(ENV_PREFIX):
            continue
        rest = key.removeprefix(ENV_PREFIX)
        section_name, sep, field_name = rest.partition("__")
        if not sep or not section_name or not field_name:
            raise ConfigError(
                f"malformed override {key!r}: expected {ENV_PREFIX}<SECTION>__<FIELD>"
            )
        section, field = section_name.lower(), field_name.lower()
        value: Any = raw
        if (section, field) in _LIST_FIELDS:
            value = [item.strip() for item in raw.split(",") if item.strip()]
        data.setdefault(section, {})[field] = value
