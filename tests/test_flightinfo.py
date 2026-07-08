"""Step 13: flight-log enrichment — synthetic truths + real-log truths.

Real-log tests use the manually-SD-copied logs in tempo-testbed/device-data
(ground truth established with scripts/flight-info.sh) and skip when that
tree is not present.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from tempo_tb_ingest.flightinfo import haversine_m, parse_flight_log

DEVICE_DATA = Path("/home/riley/src/tempo-testbed/device-data")


def write_log(tmp_path: Path, lines: list[str]) -> Path:
    p = tmp_path / "flight.txt"
    p.write_text("\r\n".join(lines) + "\r\n")
    return p


# A minimal coherent log: GGA at 12:00:00.00 paired with PTH 10000ms,
# GGA at 12:00:01.00 paired with PTH 11000ms; PST JUMPED at 11500ms
# → exit = 12:00:01.00 + (11500-11000)ms = 12:00:01.500
BASE_LINES = [
    '$PVER,"Tempo V2 1.5.0",114*72',
    "$GNRMC,120000.00,A,3326.97,N,09622.62,W,0.1,0.0,050726,,,A*00",
    "$GNGGA,120000.00,3326.97000,N,09622.62000,W,1,12,0.8,233.0,M,-23.0,M,,*00",
    "$PTH,10000*00",
    "$GNGGA,120001.00,3326.97100,N,09622.62100,W,1,12,0.8,233.0,M,-23.0,M,,*00",
    "$PTH,11000*00",
]


class TestSyntheticExit:
    def test_pst_exit_precise(self, tmp_path: Path) -> None:
        log = write_log(tmp_path, [*BASE_LINES, "$PST,11500,LOGGING,JUMPED,freefall*00"])
        info = parse_flight_log(log)
        assert info.exit_source == "PST"
        assert info.exit_utc == datetime(2026, 7, 5, 12, 0, 1, 500000, tzinfo=UTC)
        assert info.date == datetime(2026, 7, 5, tzinfo=UTC).date()
        # exit position = the GGA current at the event
        assert info.exit_lat_deg == pytest.approx(33 + 26.971 / 60)
        assert info.exit_lon_deg == pytest.approx(-(96 + 22.621 / 60))

    def test_accel_fallback_requires_ten_consecutive(self, tmp_path: Path) -> None:
        low_g = [f"$PIMU,{12000 + i * 20},0.1,0.2,0.3,0,0,0*00" for i in range(10)]
        log = write_log(tmp_path, [*BASE_LINES, *low_g])
        info = parse_flight_log(log)
        assert info.exit_source == "ACCEL"
        # first sample of the streak (12000ms) anchors the exit
        assert info.exit_utc == datetime(2026, 7, 5, 12, 0, 2, 0, tzinfo=UTC)

    def test_interrupted_low_g_streak_resets(self, tmp_path: Path) -> None:
        lines = [*BASE_LINES]
        lines += [f"$PIMU,{12000 + i * 20},0.1,0.2,0.3,0,0,0*00" for i in range(9)]
        lines.append("$PIMU,12180,0.0,0.0,9.81,0,0,0*00")  # 1 g: streak broken
        lines += [f"$PIMU,{13000 + i * 20},0.1,0.2,0.3,0,0,0*00" for i in range(10)]
        info = parse_flight_log(write_log(tmp_path, lines))
        assert info.exit_source == "ACCEL"
        assert info.exit_utc == datetime(2026, 7, 5, 12, 0, 3, 0, tzinfo=UTC)

    def test_earlier_event_wins(self, tmp_path: Path) -> None:
        low_g = [f"$PIMU,{11200 + i * 20},0.1,0.2,0.3,0,0,0*00" for i in range(10)]
        log = write_log(tmp_path, [*BASE_LINES, *low_g, "$PST,11500,LOGGING,JUMPED,freefall*00"])
        info = parse_flight_log(log)
        assert info.exit_source == "ACCEL"  # 11200 < 11500

    def test_no_exit(self, tmp_path: Path) -> None:
        info = parse_flight_log(write_log(tmp_path, BASE_LINES))
        assert info.exit_utc is None
        assert info.exit_source is None

    def test_dropkick_era_pst_deliberately_not_matched(self, tmp_path: Path) -> None:
        """Decision 2026-07-08: Tempo-only $PST (5-field transitions, JUMPED).

        The Dropkick 3-field form ($PST,<ms>,JUMPING) must not produce a PST
        exit — a legacy log falls back to accel detection instead.
        """
        log = write_log(tmp_path, [*BASE_LINES, "$PST,11500,JUMPING*00"])
        info = parse_flight_log(log)
        assert info.exit_source is None  # no accel data in fixture either

    def test_duration_and_fix_count(self, tmp_path: Path) -> None:
        info = parse_flight_log(write_log(tmp_path, BASE_LINES))
        assert info.gga_count == 2
        assert info.duration_s == pytest.approx(1.0)
        assert info.first_fix_utc == datetime(2026, 7, 5, 12, 0, 0, tzinfo=UTC)

    def test_garbage_lines_counted_not_fatal(self, tmp_path: Path) -> None:
        log = write_log(tmp_path, [*BASE_LINES, "not nmea at all", "$PTH,notanumber*00"])
        info = parse_flight_log(log)
        assert info.bad_lines == 2
        assert info.gga_count == 2


class TestHaversine:
    def test_known_distance(self) -> None:
        # ~111.2 km per degree latitude
        assert haversine_m(33.0, -96.0, 34.0, -96.0) == pytest.approx(111_195, rel=0.01)

    def test_zero(self) -> None:
        assert haversine_m(33.4569, -96.3770, 33.4569, -96.3770) == 0.0


@pytest.mark.skipif(not DEVICE_DATA.is_dir(), reason="tempo-testbed device-data not present")
class TestRealLogTruths:
    """Ground truths established with tempo-testbed/scripts/flight-info.sh."""

    @pytest.mark.parametrize(
        ("session", "source", "exit_iso"),
        [
            ("20260705/1CDD8C18", "PST", "2026-07-05T13:56:48.677"),
            ("20260705/00BAF6AB", "ACCEL", "2026-07-05T15:22:59.449"),
        ],
    )
    def test_exit_truths(self, session: str, source: str, exit_iso: str) -> None:
        path = DEVICE_DATA / "TempoBT-0001" / "logs" / session / "flight.txt"
        if not path.is_file():
            pytest.skip(f"{path} not staged")
        info = parse_flight_log(path)
        assert info.exit_source == source
        assert info.exit_utc is not None
        assert info.exit_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] == exit_iso

    def test_no_exit_truth(self) -> None:
        path = DEVICE_DATA / "TempoBT-0001" / "logs" / "20260705" / "3C44644B" / "flight.txt"
        if not path.is_file():
            pytest.skip(f"{path} not staged")
        info = parse_flight_log(path)
        assert info.exit_utc is None
        assert info.duration_s == pytest.approx(2561.0, abs=1.0)
