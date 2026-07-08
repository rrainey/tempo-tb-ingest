"""Step 10: staging store + SQLite index — atomicity, dedup, rebuild."""

import asyncio
import hashlib
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tempo_tb_ingest.events import Envelope, EventBus
from tempo_tb_ingest.owners import OwnerEntry
from tempo_tb_ingest.store import Store, StoreError

NOW = datetime(2026, 7, 8, 15, 0, 0, tzinfo=UTC)

RILEY = OwnerEntry(
    device_id="0001", device_name="Tempo-BT-0001", jumper_name="riley", is_load_organizer=True
)

CONTENT_A = b"$PVER,line\r\n" * 1000
CONTENT_B = b"$PENV,line\r\n" * 500


class Harness:
    def __init__(self, tmp_path: Path) -> None:
        self.bus = EventBus()
        self.subscription = self.bus.subscribe(queue_size=1024)
        self.staging = tmp_path / "device-data"
        self.store = Store(
            staging_root=self.staging,
            data_dir=tmp_path / "data",
            spool_dir=tmp_path / "data" / "spool",
            bus=self.bus,
        )

    def spool(self, device_id: str, key: str, content: bytes) -> Path:
        p = self.store.spool_path(device_id, key)
        p.write_bytes(content)
        return p

    def events(self) -> list[Envelope]:
        self.bus.close()

        async def drain() -> list[Envelope]:
            return [e async for e in self.subscription]

        return asyncio.run(drain())


class TestCommit:
    def test_lands_at_exact_convention_path(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        spool = h.spool("0001", "20260705/1CDD8C18", CONTENT_A)
        record = h.store.commit(
            "0001", "20260705/1CDD8C18", spool, device_name="Tempo-BT-0001", owner=RILEY, now=NOW
        )
        target = h.staging / "TempoBT-0001" / "logs" / "20260705" / "1CDD8C18" / "flight.txt"
        assert target.read_bytes() == CONTENT_A
        assert not spool.exists()  # spool consumed
        assert record.size == len(CONTENT_A)
        assert record.sha256 == hashlib.sha256(CONTENT_A).hexdigest()
        assert record.jumper == "riley"
        assert record.jumper_is_lo is True
        assert record.path == str(target)

    def test_unmapped_owner_stored_null(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        spool = h.spool("0004", "20260705/AABBCCDD", CONTENT_A)
        record = h.store.commit("0004", "20260705/AABBCCDD", spool, owner=None, now=NOW)
        assert record.jumper is None
        assert record.jumper_is_lo is False

    def test_empty_file_refused(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        spool = h.spool("0001", "20260705/1CDD8C18", b"")
        with pytest.raises(StoreError, match="empty"):
            h.store.commit("0001", "20260705/1CDD8C18", spool, now=NOW)
        assert not h.store.staging_path("0001", "20260705/1CDD8C18").exists()

    def test_malformed_key_refused(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        spool = h.spool("0001", "20260705/1CDD8C18", CONTENT_A)
        with pytest.raises(StoreError, match="malformed"):
            h.store.commit("0001", "not/a-key!", spool, now=NOW)

    def test_missing_spool_refused(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        with pytest.raises(StoreError, match="spool"):
            h.store.commit("0001", "20260705/1CDD8C18", tmp_path / "nope.part", now=NOW)

    def test_recommit_after_crash_is_idempotent(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        key = "20260705/1CDD8C18"
        h.store.commit("0001", key, h.spool("0001", key, CONTENT_A), now=NOW)
        # crash-then-retry: same content arrives again
        h.store.commit("0001", key, h.spool("0001", key, CONTENT_A), now=NOW)
        assert h.store.known_sessions("0001") == {key}
        assert h.store.staging_path("0001", key).read_bytes() == CONTENT_A

    def test_duplicate_hash_warns_but_stores(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        h.store.commit(
            "0001", "20260705/1CDD8C18", h.spool("0001", "20260705/1CDD8C18", CONTENT_A), now=NOW
        )
        h.store.commit(
            "0002", "20260705/00BAF6AB", h.spool("0002", "20260705/00BAF6AB", CONTENT_A), now=NOW
        )
        dupes = [e for e in h.events() if e.type == "store.duplicate_hash"]
        assert len(dupes) == 1
        assert dupes[0].data["duplicate_of"] == "0001/20260705/1CDD8C18"
        assert h.store.known_sessions("0002") == {"20260705/00BAF6AB"}  # stored anyway

    def test_cross_device_fallback(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        h = Harness(tmp_path)
        real_replace = __import__("os").replace
        calls = {"n": 0}

        def exdev_once(src: object, dst: object) -> None:
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError(18, "Invalid cross-device link")
            real_replace(src, dst)

        monkeypatch.setattr("tempo_tb_ingest.store.os.replace", exdev_once)
        key = "20260705/1CDD8C18"
        h.store.commit("0001", key, h.spool("0001", key, CONTENT_A), now=NOW)
        assert h.store.staging_path("0001", key).read_bytes() == CONTENT_A
        assert calls["n"] == 2  # EXDEV then copy+replace


class TestDiffing:
    def test_new_sessions_preserves_device_order(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        key = "20260705/1CDD8C18"
        h.store.commit("0001", key, h.spool("0001", key, CONTENT_A), now=NOW)
        device_keys = ["20260101/AAAAAAAA", key, "20260706/BBBBBBBB"]
        assert h.store.new_sessions("0001", device_keys) == [
            "20260101/AAAAAAAA",
            "20260706/BBBBBBBB",
        ]

    def test_per_device_isolation(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        key = "20260705/1CDD8C18"
        h.store.commit("0001", key, h.spool("0001", key, CONTENT_A), now=NOW)
        assert h.store.new_sessions("0002", [key]) == [key]  # other device: still new

    def test_totals(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        h.store.commit(
            "0001", "20260705/1CDD8C18", h.spool("0001", "20260705/1CDD8C18", CONTENT_A), now=NOW
        )
        h.store.commit(
            "0001", "20260705/00BAF6AB", h.spool("0001", "20260705/00BAF6AB", CONTENT_B), now=NOW
        )
        count, size = h.store.totals()
        assert count == 2
        assert size == len(CONTENT_A) + len(CONTENT_B)


class TestRebuild:
    def populate(self, h: Harness) -> None:
        h.store.commit(
            "0001",
            "20260705/1CDD8C18",
            h.spool("0001", "20260705/1CDD8C18", CONTENT_A),
            owner=RILEY,
            now=NOW,
        )
        h.store.commit(
            "0002", "20260705/00BAF6AB", h.spool("0002", "20260705/00BAF6AB", CONTENT_B), now=NOW
        )
        h.store.mark_promoted("0001", "20260705/1CDD8C18", "06-formation/riley")

    def test_rebuild_preserves_attribution_for_existing_rows(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        self.populate(h)
        count = h.store.rebuild_index()
        assert count == 2
        by_key = {(s.device_id, s.session_key): s for s in h.store.sessions()}
        kept = by_key[("0001", "20260705/1CDD8C18")]
        assert kept.jumper == "riley"
        assert kept.promoted_to == "06-formation/riley"
        assert kept.sha256 == hashlib.sha256(CONTENT_A).hexdigest()

    def test_rebuild_discovers_manually_added_files(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        self.populate(h)
        # a manually SD-copied session appears in the tree, unknown to the DB
        manual = h.staging / "TempoBT-0003" / "logs" / "20260201" / "F22EA218" / "flight.txt"
        manual.parent.mkdir(parents=True)
        manual.write_bytes(CONTENT_B)
        assert h.store.rebuild_index() == 3
        added = {(s.device_id, s.session_key) for s in h.store.sessions()}
        assert ("0003", "20260201/F22EA218") in added

    def test_rebuild_survives_total_db_loss(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        self.populate(h)
        h.store.close()
        (tmp_path / "data" / "ingest.db").unlink()
        reopened = Store(
            staging_root=h.staging,
            data_dir=tmp_path / "data",
            spool_dir=tmp_path / "data" / "spool",
        )
        assert reopened.rebuild_index() == 2
        sessions = reopened.sessions()
        assert {s.session_key for s in sessions} == {"20260705/1CDD8C18", "20260705/00BAF6AB"}
        assert all(s.jumper is None for s in sessions)  # attribution not derivable
        assert reopened.new_sessions("0001", ["20260705/1CDD8C18"]) == []  # diff still right
        reopened.close()

    def test_rebuild_ignores_foreign_layout(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        stray = h.staging / "TempoBT-0001" / "logs" / "notadate" / "XYZ" / "flight.txt"
        stray.parent.mkdir(parents=True)
        stray.write_bytes(CONTENT_A)
        assert h.store.rebuild_index() == 0


class TestSchema:
    def test_wal_mode(self, tmp_path: Path) -> None:
        Harness(tmp_path)
        db = sqlite3.connect(tmp_path / "data" / "ingest.db")
        assert db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        db.close()
