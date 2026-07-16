"""Step 2: configuration surface (design §3.9)."""

from pathlib import Path

import pytest

from tempo_tb_ingest.config import Config, ConfigError

# The example from docs/design.md §3.9, verbatim.
DESIGN_EXAMPLE = """\
[adapter]
scan = "hci0"            # scanning adapter
transfer = ["hci0"]      # transfer adapter pool (v1: same, single)

[detection]
rssi_floor_dbm = -88
lost_after_s = 90
absent_after_s = 600

[harvest]
connect_timeout_s = 20
max_attempts = 5
spool_dir = "/var/lib/tempo-tb-ingest/spool"

[store]
staging_root = "/home/riley/src/tempo-testbed/device-data"
data_dir = "/var/lib/tempo-tb-ingest"
# ownership registry is <staging_root>/device-owners.json (§3.12); override:
# owners_file = "..."

[promote]
test_data_root = "/home/riley/src/tempo-testbed/test-data"
exit_window_s = 120              # formation grouping window (§3.11)
gps_max_separation_m = 500      # freefall proximity cross-check

[dropzone]                       # copied verbatim into generated metadata.json
name = "Spaceland Dallas"
lat_deg = 33.4569
lon_deg = -96.3770
elevation_m = 233.0
timezone = "America/Chicago"

[http]
listen = "127.0.0.1:8080"
# static_dir = "dashboard/dist"  # built dashboard served at /; unset = API only

[log]
level = "info"           # structured JSON logs to stdout (journald)
"""


def write(tmp_path: Path, text: str) -> Path:
    p = tmp_path / "config.toml"
    p.write_text(text)
    return p


class TestDefaults:
    def test_pure_defaults(self) -> None:
        cfg = Config.load(path=None, env={})
        assert cfg.adapter.scan == "hci0"
        assert cfg.adapter.transfer == ["hci0"]
        assert cfg.detection.rssi_floor_dbm == -88
        assert cfg.detection.lost_after_s == 90.0
        assert cfg.detection.absent_after_s == 600.0
        assert cfg.harvest.connect_timeout_s == 20.0
        assert cfg.harvest.max_attempts == 5
        assert cfg.promote.exit_window_s == 120.0
        assert cfg.http.host == "127.0.0.1"
        assert cfg.http.port == 8080
        assert cfg.log.level == "info"

    def test_owners_file_defaults_next_to_staging_root(self) -> None:
        cfg = Config.load(path=None, env={})
        expected = cfg.store.staging_root / "device-owners.json"
        assert cfg.store.resolved_owners_file() == expected

    def test_owners_file_override(self, tmp_path: Path) -> None:
        p = write(tmp_path, '[store]\nowners_file = "/tmp/owners.json"\n')
        cfg = Config.load(path=p, env={})
        assert cfg.store.resolved_owners_file() == Path("/tmp/owners.json")


class TestTomlLoading:
    def test_design_doc_example_loads_verbatim(self, tmp_path: Path) -> None:
        cfg = Config.load(path=write(tmp_path, DESIGN_EXAMPLE), env={})
        assert cfg == Config.load(path=None, env={})  # the example documents the defaults

    def test_partial_file_keeps_other_defaults(self, tmp_path: Path) -> None:
        p = write(tmp_path, "[detection]\nabsent_after_s = 30\n")
        cfg = Config.load(path=p, env={})
        assert cfg.detection.absent_after_s == 30.0
        assert cfg.detection.lost_after_s == 90.0  # untouched default

    def test_explicit_missing_path_is_an_error(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="not found"):
            Config.load(path=tmp_path / "nope.toml", env={})

    def test_malformed_toml_is_an_error(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="cannot read"):
            Config.load(path=write(tmp_path, "[adapter\n"), env={})

    def test_unknown_key_is_an_error(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError):
            Config.load(path=write(tmp_path, "[detection]\ntypo_field = 1\n"), env={})

    def test_unknown_section_is_an_error(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError):
            Config.load(path=write(tmp_path, "[radios]\nscan = 'hci0'\n"), env={})


class TestEnvOverrides:
    def test_env_beats_file(self, tmp_path: Path) -> None:
        p = write(tmp_path, "[detection]\nabsent_after_s = 600\n")
        cfg = Config.load(path=p, env={"TEMPO_INGEST_DETECTION__ABSENT_AFTER_S": "30"})
        assert cfg.detection.absent_after_s == 30.0

    def test_env_on_pure_defaults(self) -> None:
        cfg = Config.load(path=None, env={"TEMPO_INGEST_HTTP__LISTEN": "0.0.0.0:9000"})
        assert cfg.http.port == 9000

    def test_list_field_splits_on_commas(self) -> None:
        cfg = Config.load(path=None, env={"TEMPO_INGEST_ADAPTER__TRANSFER": "hci1, hci2"})
        assert cfg.adapter.transfer == ["hci1", "hci2"]

    def test_unrelated_env_ignored(self) -> None:
        cfg = Config.load(path=None, env={"PATH": "/usr/bin", "TEMPO_OTHER": "x"})
        assert cfg == Config.load(path=None, env={})

    def test_malformed_override_name_is_an_error(self) -> None:
        with pytest.raises(ConfigError, match="malformed override"):
            Config.load(path=None, env={"TEMPO_INGEST_ABSENT_AFTER_S": "30"})

    def test_bad_env_value_is_an_error(self) -> None:
        with pytest.raises(ConfigError):
            Config.load(path=None, env={"TEMPO_INGEST_DETECTION__ABSENT_AFTER_S": "soon"})


class TestValidation:
    @pytest.mark.parametrize(
        "toml_text",
        [
            "[detection]\nlost_after_s = -5\n",
            "[detection]\nabsent_after_s = 0\n",
            "[detection]\nrssi_floor_dbm = 40\n",
            "[harvest]\nconnect_timeout_s = 0\n",
            "[harvest]\nmax_attempts = 0\n",
            '[adapter]\nscan = ""\n',
            "[adapter]\ntransfer = []\n",
            '[adapter]\ntransfer = [""]\n',
            '[http]\nlisten = "8080"\n',
            '[http]\nlisten = "localhost:0"\n',
            '[http]\nlisten = "localhost:99999"\n',
            '[log]\nlevel = "loud"\n',
            "[dropzone]\nlat_deg = 123.0\n",
            '[dropzone]\ntimezone = ""\n',
            "[promote]\nexit_window_s = 0\n",
        ],
    )
    def test_bad_values_rejected(self, tmp_path: Path, toml_text: str) -> None:
        with pytest.raises(ConfigError):
            Config.load(path=write(tmp_path, toml_text), env={})

    def test_log_level_case_normalized(self, tmp_path: Path) -> None:
        cfg = Config.load(path=write(tmp_path, '[log]\nlevel = "INFO"\n'), env={})
        assert cfg.log.level == "info"
