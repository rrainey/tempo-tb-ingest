"""Step 13: formation grouping, case proposals, apply — incl. the golden test
against the real multi-device 20260206 logs (ground truth: the hand-built
test-data cases 02/03/04)."""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import ClassVar

import pytest

from tempo_tb_ingest.config import Config
from tempo_tb_ingest.flightinfo import FlightInfo
from tempo_tb_ingest.owners import OwnerEntry, OwnersRegistry
from tempo_tb_ingest.promote import (
    Enriched,
    apply_proposal,
    build_proposal,
    enrich_one,
    group_formations,
    next_case_number,
    reattribute,
    render_proposal,
)
from tempo_tb_ingest.store import Store, StoredSession

DEVICE_DATA = Path("/home/riley/src/tempo-testbed/device-data")

# --------------------------------------------------------------------------- #
# synthetic log builder (exit time and position fully controlled)


def make_log(
    hh: int,
    mm: int,
    ss: int,
    lat_min: float,
    lon_min: float,
    *,
    exit_delta_ms: int | None = 500,
    filler: str = "",
) -> bytes:
    t0 = f"{hh:02d}{mm:02d}{ss:02d}.00"
    t1 = f"{hh:02d}{mm:02d}{ss + 1:02d}.00"
    lines = [
        '$PVER,"Tempo V2 1.5.0",114*72',
        f"$GNRMC,{t0},A,3326.97,N,09622.62,W,0.1,0.0,050726,,,A*00",
        f"$GNGGA,{t0},33{lat_min:08.5f},N,096{lon_min:08.5f},W,1,12,0.8,233.0,M,-23.0,M,,*00",
        "$PTH,10000*00",
        f"$GNGGA,{t1},33{lat_min + 0.001:08.5f},N,096{lon_min:08.5f},W,1,12,0.8,"
        "233.0,M,-23.0,M,,*00",
        "$PTH,11000*00",
    ]
    if exit_delta_ms is not None:
        lines.append(f"$PST,{11000 + exit_delta_ms},LOGGING,JUMPED,freefall*00")
    if filler:
        lines.append(f"$PENV,{filler}*00")
    return ("\r\n".join(lines) + "\r\n").encode()


def info_at(
    exit_utc: datetime | None, lat: float | None = 33.45, lon: float | None = -96.37
) -> FlightInfo:
    return FlightInfo(
        date=exit_utc.date() if exit_utc else None,
        first_fix_utc=exit_utc,
        last_fix_utc=exit_utc,
        duration_s=1000.0,
        exit_utc=exit_utc,
        exit_source="PST" if exit_utc else None,
        exit_lat_deg=lat if exit_utc else None,
        exit_lon_deg=lon if exit_utc else None,
        gga_count=2,
        bad_lines=0,
    )


def enriched(
    device: str, key: str, jumper: str, info: FlightInfo, is_lo: bool = False
) -> Enriched:
    session = StoredSession(
        device_id=device,
        session_key=key,
        size=1000,
        sha256="0" * 64,
        downloaded_at="2026-07-08T00:00:00+00:00",
        path=f"/nonexistent/{device}/{key}/flight.txt",
        jumper=jumper,
        jumper_is_lo=is_lo,
        promoted_to=None,
    )
    return Enriched(session=session, info=info)


def at(hh: int, mm: int, ss: float) -> datetime:
    return datetime(2026, 7, 5, hh, mm, int(ss), int((ss % 1) * 1e6), tzinfo=UTC)


class TestGrouping:
    def test_close_exits_group(self) -> None:
        a = enriched("0001", "20260705/AAAAAAAA", "riley", info_at(at(15, 22, 59.4)))
        b = enriched("0007", "20260705/BBBBBBBB", "scott_z", info_at(at(15, 22, 58.1)))
        groups = group_formations([a, b], exit_window_s=120, gps_max_separation_m=500)
        assert len(groups) == 1
        assert {e.session.jumper for e in groups[0]} == {"riley", "scott_z"}

    def test_window_separates_loads(self) -> None:
        a = enriched("0001", "20260705/AAAAAAAA", "riley", info_at(at(13, 56, 48)))
        b = enriched("0007", "20260705/BBBBBBBB", "scott_z", info_at(at(15, 22, 58)))
        groups = group_formations([a, b], exit_window_s=120, gps_max_separation_m=500)
        assert len(groups) == 2

    def test_gps_splits_coincident_window(self) -> None:
        # same exit minute, but 5+ km apart: two aircraft
        a = enriched("0001", "20260705/AAAAAAAA", "riley", info_at(at(15, 0, 0), 33.45, -96.37))
        b = enriched("0007", "20260705/BBBBBBBB", "scott_z", info_at(at(15, 0, 10), 33.50, -96.37))
        groups = group_formations([a, b], exit_window_s=120, gps_max_separation_m=500)
        assert len(groups) == 2

    def test_missing_position_cannot_refute_time_grouping(self) -> None:
        a = enriched("0001", "20260705/AAAAAAAA", "riley", info_at(at(15, 0, 0)))
        b = enriched("0007", "20260705/BBBBBBBB", "scott_z", info_at(at(15, 0, 10), None, None))
        groups = group_formations([a, b], exit_window_s=120, gps_max_separation_m=500)
        assert len(groups) == 1

    def test_no_exit_never_reaches_grouping(self) -> None:
        a = enriched("0001", "20260705/AAAAAAAA", "riley", info_at(None))
        groups = group_formations([a], exit_window_s=120, gps_max_separation_m=500)
        assert groups == []

    def test_chain_clustering_by_gap(self) -> None:
        # exits at t, t+100, t+200: pairwise gaps within window chain together
        entries = [
            enriched("0001", "20260705/AAAAAAAA", "a", info_at(at(15, 0, 0))),
            enriched("0002", "20260705/BBBBBBBB", "b", info_at(at(15, 1, 40))),
            enriched("0003", "20260705/CCCCCCCC", "c", info_at(at(15, 3, 20))),
        ]
        groups = group_formations(entries, exit_window_s=120, gps_max_separation_m=500)
        assert len(groups) == 1
        groups = group_formations(entries, exit_window_s=60, gps_max_separation_m=500)
        assert len(groups) == 3


@pytest.mark.skipif(not DEVICE_DATA.is_dir(), reason="tempo-testbed device-data not present")
class TestGoldenRealGrouping:
    """The 20260206 jump day: 3 formations, hand-verified in test-data 02/03/04."""

    DEVICE_JUMPER: ClassVar[dict[str, str]] = {
        "TempoBT-0001": "riley",
        "TempoBT-0002": "russ",
        "TempoBT-0003": "divyatej_dt",
    }
    EXPECTED: ClassVar[list[set[str]]] = [
        {"D243913E", "FBCE83F5", "FBC3C6C5"},  # jump 1: 3-way
        {"9CECECF2", "93D28166"},  # jump 2: 2-way
        {"51F70073", "C706A47C"},  # jump 3: 2-way
    ]

    def test_reproduces_hand_built_grouping(self) -> None:
        entries: list[Enriched] = []
        for folder, jumper in self.DEVICE_JUMPER.items():
            for flight in sorted(DEVICE_DATA.glob(f"{folder}/logs/20260206/*/flight.txt")):
                key = f"20260206/{flight.parent.name}"
                session = StoredSession(
                    device_id=folder.removeprefix("TempoBT-"),
                    session_key=key,
                    size=flight.stat().st_size,
                    sha256="",
                    downloaded_at="",
                    path=str(flight),
                    jumper=jumper,
                    jumper_is_lo=False,
                    promoted_to=None,
                )
                entries.append(enrich_one(session))
        if len(entries) < 7:
            pytest.skip("20260206 logs not fully staged")
        # V110 logs carry no $GNRMC: exits must still resolve via the key date
        assert all(e.info.exit_utc is not None for e in entries)
        groups = group_formations(entries, exit_window_s=120, gps_max_separation_m=500)
        got = [{e.session.session_key.split("/")[1] for e in group} for group in groups]
        assert got == self.EXPECTED


# --------------------------------------------------------------------------- #
# proposals and apply (tmp trees, synthetic logs)


class Rig:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp = tmp_path
        self.test_data = tmp_path / "test-data"
        self.test_data.mkdir()
        self.store = Store(
            staging_root=tmp_path / "device-data",
            data_dir=tmp_path / "data",
            spool_dir=tmp_path / "data" / "spool",
        )
        self.config = Config.model_validate(
            {
                "store": {
                    "staging_root": str(tmp_path / "device-data"),
                    "data_dir": str(tmp_path / "data"),
                },
                "promote": {"test_data_root": str(self.test_data)},
            }
        )

    def commit(
        self, device: str, key: str, content: bytes, jumper: str | None, is_lo: bool = False
    ) -> None:
        spool = self.store.spool_path(device, key)
        spool.write_bytes(content)
        owner = (
            OwnerEntry(
                device_id=device,
                device_name=f"Tempo-BT-{device}",
                jumper_name=jumper,
                is_load_organizer=is_lo,
            )
            if jumper
            else None
        )
        self.store.commit(device, key, spool, owner=owner)


def two_way_rig(tmp_path: Path) -> Rig:
    rig = Rig(tmp_path)
    # 15:22-ish exits ~40m apart: one 2-way; scott_z is LO
    rig.commit(
        "0001", "20260705/AAAAAAAA", make_log(15, 22, 58, 26.95340, 22.55286, filler="a"), "riley"
    )
    rig.commit(
        "0007",
        "20260705/BBBBBBBB",
        make_log(15, 22, 57, 26.94606, 22.52808, filler="b"),
        "scott_z",
        is_lo=True,
    )
    # 13:56 solo for riley
    rig.commit(
        "0001", "20260705/CCCCCCCC", make_log(13, 56, 48, 26.95540, 22.04920, filler="c"), "riley"
    )
    # a no-exit session
    rig.commit(
        "0001",
        "20260705/DDDDDDDD",
        make_log(16, 30, 0, 26.95, 22.05, exit_delta_ms=None, filler="d"),
        "riley",
    )
    # an unmapped session
    rig.commit("0004", "20260705/EEEEEEEE", make_log(15, 22, 59, 26.95, 22.55, filler="e"), None)
    return rig


class TestProposal:
    def test_cases_flags_and_ordering(self, tmp_path: Path) -> None:
        rig = two_way_rig(tmp_path)
        proposal = build_proposal(rig.store, rig.config)

        assert len(proposal.cases) == 2
        solo, formation = proposal.cases  # chronological: 13:56 solo first
        assert solo.is_solo and solo.dirname == "01-solo-riley-20260705"
        assert formation.dirname == "02-formation-20260705-2way"
        assert formation.base_jumper == "scott_z"  # the LO is the base
        assert formation.metadata["jumpers"] == ["riley", "scott_z"]
        assert formation.metadata["isSolo"] is False
        assert [e.session.session_key for e in proposal.no_exit] == ["20260705/DDDDDDDD"]
        assert [s.session_key for s in proposal.unmapped] == ["20260705/EEEEEEEE"]
        assert proposal.flags  # unmapped warning present

    def test_metadata_schema_complete(self, tmp_path: Path) -> None:
        rig = two_way_rig(tmp_path)
        proposal = build_proposal(rig.store, rig.config)
        md = proposal.cases[1].metadata
        assert set(md) == {
            "name",
            "description",
            "dropzone",
            "jumpers",
            "baseJumper",
            "isSolo",
            "tags",
        }
        dz = md["dropzone"]
        assert isinstance(dz, dict)
        assert set(dz) == {"name", "lat_deg", "lon_deg", "elevation_m", "timezone"}
        assert "AAAAAAAA (riley)" in str(md["description"])
        assert "2way" in md["tags"]  # type: ignore[operator]

    def test_numbering_continues_after_existing_cases(self, tmp_path: Path) -> None:
        rig = two_way_rig(tmp_path)
        (rig.test_data / "05-formation-jump4-3way").mkdir()
        proposal = build_proposal(rig.store, rig.config)
        assert proposal.cases[0].dirname.startswith("06-")
        assert next_case_number(rig.test_data) == 6

    def test_no_lo_flagged_and_defaulted(self, tmp_path: Path) -> None:
        rig = Rig(tmp_path)
        rig.commit(
            "0001", "20260705/AAAAAAAA", make_log(15, 0, 0, 26.95, 22.55, filler="a"), "riley"
        )
        rig.commit(
            "0002", "20260705/BBBBBBBB", make_log(15, 0, 1, 26.951, 22.551, filler="b"), "russ"
        )
        proposal = build_proposal(rig.store, rig.config)
        case = proposal.cases[0]
        assert case.base_jumper == "riley"  # alphabetical default
        assert any("no load organizer" in f for f in case.flags)

    def test_duplicate_jumper_in_group_flagged(self, tmp_path: Path) -> None:
        rig = Rig(tmp_path)
        rig.commit(
            "0001", "20260705/AAAAAAAA", make_log(15, 0, 0, 26.95, 22.55, filler="a"), "riley"
        )
        rig.commit(
            "0009", "20260705/BBBBBBBB", make_log(15, 0, 1, 26.951, 22.551, filler="b"), "riley"
        )
        proposal = build_proposal(rig.store, rig.config)
        case = proposal.cases[0]
        assert case.is_solo  # one jumper kept
        assert any("two sessions" in f for f in case.flags)

    def test_render_mentions_everything(self, tmp_path: Path) -> None:
        rig = two_way_rig(tmp_path)
        text = render_proposal(build_proposal(rig.store, rig.config))
        for needle in ("02-formation-20260705-2way", "scott_z (base)", "NOT PROPOSED", "UNMAPPED"):
            assert needle in text


class TestSameTakeoffHints:
    """Issue #1 (2026-07-10): real July-3 data had two same-load logs whose
    exits differed by 7m40s — grouping was correct, detection wasn't; the
    proposal must surface it."""

    def test_split_cases_sharing_takeoff_flagged(self, tmp_path: Path) -> None:
        rig = Rig(tmp_path)
        # identical first fix (16:14:55-analog), exits ~7.7 min apart
        rig.commit(
            "0002", "20260703/AAAAAAAA", make_log(16, 14, 55, 26.95, 22.55, filler="a"), "riley"
        )
        rig.commit(
            "0003",
            "20260703/BBBBBBBB",
            make_log(16, 14, 55, 26.951, 22.551, exit_delta_ms=460_000, filler="b"),
            "bb",
        )
        proposal = build_proposal(rig.store, rig.config)
        assert len(proposal.cases) == 2  # correctly NOT grouped
        assert any("share a takeoff" in f and "7.7 min" in f for f in proposal.flags)

    def test_no_exit_session_sharing_takeoff_flagged(self, tmp_path: Path) -> None:
        rig = Rig(tmp_path)
        rig.commit(
            "0001", "20260705/AAAAAAAA", make_log(13, 43, 16, 26.95, 22.55, filler="a"), "riley"
        )
        rig.commit(
            "0007",
            "20260705/BBBBBBBB",
            make_log(13, 43, 20, 26.951, 22.551, exit_delta_ms=None, filler="b"),
            "scott_z",
        )
        proposal = build_proposal(rig.store, rig.config)
        assert any("shares a takeoff" in f and "undetected" in f for f in proposal.flags)

    def test_unrelated_loads_not_flagged(self, tmp_path: Path) -> None:
        rig = two_way_rig(tmp_path)
        proposal = build_proposal(rig.store, rig.config)
        assert not any("takeoff" in f for f in proposal.flags)


class TestApply:
    def test_apply_copies_and_marks(self, tmp_path: Path) -> None:
        rig = two_way_rig(tmp_path)
        proposal = build_proposal(rig.store, rig.config)
        created = apply_proposal(proposal, rig.store, rig.config)
        assert [p.name for p in created] == [
            "01-solo-riley-20260705",
            "02-formation-20260705-2way",
        ]

        case_dir = rig.test_data / "02-formation-20260705-2way"
        md = json.loads((case_dir / "metadata.json").read_text())
        assert md["baseJumper"] == "scott_z"
        copied = (case_dir / "riley" / "flight.txt").read_bytes()
        original = Path(rig.store.staging_path("0001", "20260705/AAAAAAAA")).read_bytes()
        assert copied == original
        assert rig.store.staging_path("0001", "20260705/AAAAAAAA").exists()  # staging intact

        promoted = {s.session_key: s.promoted_to for s in rig.store.sessions()}
        assert promoted["20260705/AAAAAAAA"] == "02-formation-20260705-2way/riley"
        assert promoted["20260705/DDDDDDDD"] is None  # no-exit untouched

    def test_second_run_proposes_nothing_new(self, tmp_path: Path) -> None:
        rig = two_way_rig(tmp_path)
        apply_proposal(build_proposal(rig.store, rig.config), rig.store, rig.config)
        again = build_proposal(rig.store, rig.config)
        assert again.cases == []  # promoted sessions excluded
        assert len(again.no_exit) == 1  # still listed, still unpromoted

    def test_collision_refused(self, tmp_path: Path) -> None:
        rig = two_way_rig(tmp_path)
        proposal = build_proposal(rig.store, rig.config)
        (rig.test_data / proposal.cases[0].dirname).mkdir()
        with pytest.raises(FileExistsError):
            apply_proposal(proposal, rig.store, rig.config)


class TestReattribute:
    def test_rebinds_unmapped_unpromoted_only(self, tmp_path: Path) -> None:
        rig = two_way_rig(tmp_path)
        registry_path = tmp_path / "device-owners.json"
        registry_path.write_text(
            json.dumps([{"deviceName": "Tempo-BT-0004", "jumperName": "billy"}])
        )
        updated = reattribute(rig.store, OwnersRegistry(registry_path))
        assert updated == 1
        by_key = {s.session_key: s for s in rig.store.sessions("0004")}
        assert by_key["20260705/EEEEEEEE"].jumper == "billy"
        # and now it becomes proposable
        proposal = build_proposal(rig.store, rig.config)
        assert proposal.unmapped == []
