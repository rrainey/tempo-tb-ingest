"""Staging-tree writer and SQLite session index (design §3.6).

The staging tree (``<staging_root>/TempoBT-<id>/logs/<key>/flight.txt``) is
the human-browsable source of truth for file *content*; the SQLite DB is the
daemon's memory (diffing, dedup, attribution, promote bookkeeping) and is
always disposable: ``rebuild_index()`` reconstructs it by walking the tree.
Harvest-time-only facts (jumper attribution, promotion state) are preserved
for rows that still exist; with a wholly lost DB they cannot be rederived.

Commit is atomic: the spool file is moved into place with ``os.replace``
(same filesystem) or copy+fsync+replace (cross-device) — a partial file is
never visible at the final path.
"""

import hashlib
import os
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from tempo_tb_ingest.device.protocol import SESSION_KEY_RE
from tempo_tb_ingest.events import EventBus, StoreDuplicateHash
from tempo_tb_ingest.owners import OwnerEntry

_SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
  device_id TEXT PRIMARY KEY,
  name TEXT,
  folder TEXT,
  last_mac TEXT,
  first_seen TEXT,
  last_seen TEXT,
  notes TEXT
);
CREATE TABLE IF NOT EXISTS sessions (
  device_id TEXT NOT NULL,
  session_key TEXT NOT NULL,
  size INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  downloaded_at TEXT NOT NULL,
  path TEXT NOT NULL,
  jumper TEXT,
  jumper_is_lo INTEGER NOT NULL DEFAULT 0,
  promoted_to TEXT,
  PRIMARY KEY (device_id, session_key)
);
CREATE INDEX IF NOT EXISTS sessions_sha ON sessions(sha256);
"""


class StoreError(Exception):
    """A commit that must not proceed (empty file, malformed key, IO error)."""


@dataclass(frozen=True)
class StoredSession:
    device_id: str
    session_key: str
    size: int
    sha256: str
    downloaded_at: str
    path: str
    jumper: str | None
    jumper_is_lo: bool
    promoted_to: str | None


def device_folder(device_id: str) -> str:
    """Staging folder convention: id ``0001`` → ``TempoBT-0001``."""
    return f"TempoBT-{device_id}"


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


class Store:
    def __init__(
        self,
        staging_root: Path,
        data_dir: Path,
        spool_dir: Path,
        bus: EventBus | None = None,
    ) -> None:
        self.staging_root = staging_root
        self.spool_dir = spool_dir
        self._bus = bus
        data_dir.mkdir(parents=True, exist_ok=True)
        spool_dir.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(data_dir / "ingest.db")
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(_SCHEMA)
        self._db.commit()

    def close(self) -> None:
        self._db.close()

    # -- querying -------------------------------------------------------------

    def known_sessions(self, device_id: str) -> set[str]:
        rows = self._db.execute(
            "SELECT session_key FROM sessions WHERE device_id = ?", (device_id,)
        )
        return {key for (key,) in rows}

    def new_sessions(self, device_id: str, device_keys: list[str]) -> list[str]:
        """Device order preserved; unknown-to-us sessions only."""
        known = self.known_sessions(device_id)
        return [k for k in device_keys if k not in known]

    def sessions(self, device_id: str | None = None) -> list[StoredSession]:
        query = (
            "SELECT device_id, session_key, size, sha256, downloaded_at, path,"
            " jumper, jumper_is_lo, promoted_to FROM sessions"
        )
        args: tuple[str, ...] = ()
        if device_id is not None:
            query += " WHERE device_id = ?"
            args = (device_id,)
        query += " ORDER BY device_id, session_key"
        return [
            StoredSession(
                device_id=r[0],
                session_key=r[1],
                size=r[2],
                sha256=r[3],
                downloaded_at=r[4],
                path=r[5],
                jumper=r[6],
                jumper_is_lo=bool(r[7]),
                promoted_to=r[8],
            )
            for r in self._db.execute(query, args)
        ]

    def totals(self) -> tuple[int, int]:
        row = self._db.execute("SELECT COUNT(*), COALESCE(SUM(size), 0) FROM sessions").fetchone()
        return int(row[0]), int(row[1])

    # -- spooling & committing --------------------------------------------------

    def spool_path(self, device_id: str, session_key: str) -> Path:
        safe_key = session_key.replace("/", "-")
        return self.spool_dir / f"{device_id}-{safe_key}.part"

    def staging_path(self, device_id: str, session_key: str) -> Path:
        return self.staging_root / device_folder(device_id) / "logs" / session_key / "flight.txt"

    def commit(
        self,
        device_id: str,
        session_key: str,
        spool_file: Path,
        *,
        device_name: str | None = None,
        mac: str | None = None,
        owner: OwnerEntry | None = None,
        now: datetime | None = None,
    ) -> StoredSession:
        """Verify, atomically move into staging, record in the index."""
        if not SESSION_KEY_RE.match(session_key):
            raise StoreError(f"malformed session key {session_key!r}")
        if not spool_file.is_file():
            raise StoreError(f"spool file missing: {spool_file}")
        size = spool_file.stat().st_size
        if size == 0:
            raise StoreError(f"refusing to store empty file for {device_id}/{session_key}")
        digest = sha256_of(spool_file)
        timestamp = (now or datetime.now(UTC)).isoformat()

        duplicate = self._db.execute(
            "SELECT device_id, session_key FROM sessions WHERE sha256 = ?"
            " AND NOT (device_id = ? AND session_key = ?)",
            (digest, device_id, session_key),
        ).fetchone()
        if duplicate is not None and self._bus is not None:
            self._bus.publish(
                StoreDuplicateHash(
                    id=device_id,
                    session_key=session_key,
                    sha256=digest,
                    duplicate_of=f"{duplicate[0]}/{duplicate[1]}",
                )
            )

        target = self.staging_path(device_id, session_key)
        target.parent.mkdir(parents=True, exist_ok=True)
        _atomic_move(spool_file, target)

        record = StoredSession(
            device_id=device_id,
            session_key=session_key,
            size=size,
            sha256=digest,
            downloaded_at=timestamp,
            path=str(target),
            jumper=owner.jumper_name if owner else None,
            jumper_is_lo=owner.is_load_organizer if owner else False,
            promoted_to=None,
        )
        self._db.execute(
            "INSERT INTO sessions (device_id, session_key, size, sha256, downloaded_at,"
            " path, jumper, jumper_is_lo, promoted_to) VALUES (?,?,?,?,?,?,?,?,NULL)"
            " ON CONFLICT(device_id, session_key) DO UPDATE SET size=excluded.size,"
            " sha256=excluded.sha256, downloaded_at=excluded.downloaded_at,"
            " path=excluded.path, jumper=excluded.jumper, jumper_is_lo=excluded.jumper_is_lo",
            (
                record.device_id,
                record.session_key,
                record.size,
                record.sha256,
                record.downloaded_at,
                record.path,
                record.jumper,
                int(record.jumper_is_lo),
            ),
        )
        self._upsert_device(device_id, device_name, mac, timestamp)
        self._db.commit()
        return record

    def mark_promoted(self, device_id: str, session_key: str, promoted_to: str) -> None:
        self._db.execute(
            "UPDATE sessions SET promoted_to = ? WHERE device_id = ? AND session_key = ?",
            (promoted_to, device_id, session_key),
        )
        self._db.commit()

    def update_attribution(
        self, device_id: str, session_key: str, jumper: str, is_lo: bool
    ) -> None:
        """Re-bind a session's jumper (promote --reattribute, design §3.11)."""
        self._db.execute(
            "UPDATE sessions SET jumper = ?, jumper_is_lo = ?"
            " WHERE device_id = ? AND session_key = ?",
            (jumper, int(is_lo), device_id, session_key),
        )
        self._db.commit()

    # -- maintenance ------------------------------------------------------------

    def rebuild_index(self) -> int:
        """Reconstruct the sessions table by walking the staging tree.

        Hashes are recomputed from file content. Harvest-time facts (jumper,
        promotion) are preserved for rows already present in the DB; for
        files with no surviving row they cannot be rederived and are NULL.
        """
        preserved = {(s.device_id, s.session_key): s for s in self.sessions()}
        self._db.execute("DELETE FROM sessions")
        count = 0
        for flight in sorted(self.staging_root.glob("TempoBT-*/logs/*/*/flight.txt")):
            session_key = f"{flight.parent.parent.name}/{flight.parent.name}"
            device_id = flight.parent.parent.parent.parent.name.removeprefix("TempoBT-")
            if not SESSION_KEY_RE.match(session_key) or len(device_id) != 4:
                continue
            size = flight.stat().st_size
            old = preserved.get((device_id, session_key))
            self._db.execute(
                "INSERT INTO sessions (device_id, session_key, size, sha256, downloaded_at,"
                " path, jumper, jumper_is_lo, promoted_to) VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    device_id,
                    session_key,
                    size,
                    sha256_of(flight),
                    old.downloaded_at if old else datetime.now(UTC).isoformat(),
                    str(flight),
                    old.jumper if old else None,
                    int(old.jumper_is_lo) if old else 0,
                    old.promoted_to if old else None,
                ),
            )
            count += 1
        self._db.commit()
        return count

    # -- internals ---------------------------------------------------------------

    def _upsert_device(
        self, device_id: str, name: str | None, mac: str | None, timestamp: str
    ) -> None:
        self._db.execute(
            "INSERT INTO devices (device_id, name, folder, last_mac, first_seen, last_seen)"
            " VALUES (?,?,?,?,?,?)"
            " ON CONFLICT(device_id) DO UPDATE SET name=COALESCE(excluded.name, name),"
            " last_mac=COALESCE(excluded.last_mac, last_mac), last_seen=excluded.last_seen",
            (device_id, name, device_folder(device_id), mac, timestamp, timestamp),
        )


def _atomic_move(source: Path, target: Path) -> None:
    try:
        os.replace(source, target)
    except OSError as exc:
        if exc.errno != 18:  # EXDEV: cross-device link
            raise StoreError(f"cannot move {source} -> {target}: {exc}") from exc
        tmp = target.with_suffix(".tmp")
        shutil.copyfile(source, tmp)
        with tmp.open("rb+") as fh:
            os.fsync(fh.fileno())
        os.replace(tmp, target)
        source.unlink()
