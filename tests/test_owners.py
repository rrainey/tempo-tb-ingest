"""Step 10: device-owners.json registry — parse, validate, hot-reload, loudness."""

import asyncio
import json
import os
from pathlib import Path

from tempo_tb_ingest.events import Envelope, EventBus
from tempo_tb_ingest.owners import OwnersRegistry

VALID = [
    {"deviceName": "Tempo-BT-0001", "jumperName": "riley", "isLoadOrganizer": True},
    {"deviceName": "Tempo-BT-0002", "jumperName": "russ"},
    {"deviceName": "Tempo-BT-0003", "jumperName": "divyatej_dt"},
]


class Harness:
    def __init__(self, tmp_path: Path) -> None:
        self.path = tmp_path / "device-owners.json"
        self.bus = EventBus()
        self.subscription = self.bus.subscribe(queue_size=1024)
        self.registry = OwnersRegistry(self.path, self.bus)

    def write(self, content: object, mtime_bump: float = 10.0) -> None:
        text = content if isinstance(content, str) else json.dumps(content)
        self.path.write_text(text)
        # ensure a strictly newer mtime even on coarse filesystems
        stat = self.path.stat()
        os.utime(self.path, (stat.st_atime, stat.st_mtime + mtime_bump))

    def events(self) -> list[Envelope]:
        self.bus.close()

        async def drain() -> list[Envelope]:
            return [e async for e in self.subscription]

        return asyncio.run(drain())


class TestParsing:
    def test_valid_registry(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        h.write(VALID)
        entry = h.registry.lookup("0001")
        assert entry is not None
        assert entry.jumper_name == "riley"
        assert entry.is_load_organizer is True
        entry2 = h.registry.lookup("0002")
        assert entry2 is not None
        assert entry2.is_load_organizer is False  # default
        assert h.registry.lookup("0099") is None  # unmapped
        events = h.events()
        assert [e.type for e in events] == ["owners.reloaded"]
        assert events[0].data["entries"] == 3

    def test_same_jumper_on_two_devices_is_legal(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        h.write(
            [
                {"deviceName": "Tempo-BT-0001", "jumperName": "riley"},
                {"deviceName": "Tempo-BT-0010", "jumperName": "riley"},
            ]
        )
        assert h.registry.lookup("0001") is not None
        assert h.registry.lookup("0010") is not None

    def test_missing_file_is_loud_once(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        assert h.registry.lookup("0001") is None
        assert h.registry.lookup("0002") is None  # second lookup: no repeat event
        events = h.events()
        assert [e.type for e in events] == ["owners.error"]
        assert "not found" in events[0].data["reason"]


class TestValidationErrors:
    def check_rejected(self, tmp_path: Path, content: object, match: str) -> None:
        h = Harness(tmp_path)
        h.write(content)
        assert h.registry.lookup("0001") is None
        errors = [e for e in h.events() if e.type == "owners.error"]
        assert len(errors) == 1
        assert match in errors[0].data["reason"]

    def test_garbage_json(self, tmp_path: Path) -> None:
        self.check_rejected(tmp_path, "{not json", "unparseable")

    def test_not_a_list(self, tmp_path: Path) -> None:
        self.check_rejected(tmp_path, {"deviceName": "Tempo-BT-0001"}, "array")

    def test_duplicate_device(self, tmp_path: Path) -> None:
        self.check_rejected(
            tmp_path,
            [
                {"deviceName": "Tempo-BT-0001", "jumperName": "riley"},
                {"deviceName": "Tempo-BT-0001", "jumperName": "russ"},
            ],
            "duplicate",
        )

    def test_bad_device_name(self, tmp_path: Path) -> None:
        self.check_rejected(
            tmp_path, [{"deviceName": "Tempo-BT", "jumperName": "riley"}], "suffix"
        )

    def test_bad_jumper_name(self, tmp_path: Path) -> None:
        self.check_rejected(
            tmp_path,
            [{"deviceName": "Tempo-BT-0001", "jumperName": "Riley Rainey!"}],
            "jumperName",
        )

    def test_unknown_field(self, tmp_path: Path) -> None:
        self.check_rejected(
            tmp_path,
            [{"deviceName": "Tempo-BT-0001", "jumperName": "riley", "isLO": True}],
            "unknown field",
        )

    def test_non_bool_lo(self, tmp_path: Path) -> None:
        self.check_rejected(
            tmp_path,
            [{"deviceName": "Tempo-BT-0001", "jumperName": "riley", "isLoadOrganizer": "yes"}],
            "boolean",
        )


class TestHotReload:
    def test_edit_takes_effect_on_next_lookup(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        h.write(VALID)
        assert h.registry.lookup("0001").jumper_name == "riley"  # type: ignore[union-attr]
        # the device changes hands mid-day
        h.write([{"deviceName": "Tempo-BT-0001", "jumperName": "billy"}])
        assert h.registry.lookup("0001").jumper_name == "billy"  # type: ignore[union-attr]
        assert h.registry.lookup("0002") is None  # removed entry gone
        reloads = [e for e in h.events() if e.type == "owners.reloaded"]
        assert len(reloads) == 2

    def test_unchanged_file_not_reparsed(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        h.write(VALID)
        for _ in range(5):
            h.registry.lookup("0001")
        assert [e.type for e in h.events()] == ["owners.reloaded"]

    def test_bad_edit_keeps_last_good_copy(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        h.write(VALID)
        assert h.registry.lookup("0001") is not None
        h.write("{broken")
        # last good copy remains in use; the bad revision reported once
        assert h.registry.lookup("0001").jumper_name == "riley"  # type: ignore[union-attr]
        assert h.registry.lookup("0001") is not None
        events = h.events()
        assert [e.type for e in events] == ["owners.reloaded", "owners.error"]

    def test_recovery_after_bad_edit(self, tmp_path: Path) -> None:
        h = Harness(tmp_path)
        h.write(VALID)
        h.registry.lookup("0001")
        h.write("{broken")
        h.registry.lookup("0001")
        h.write([{"deviceName": "Tempo-BT-0001", "jumperName": "billy"}], mtime_bump=20.0)
        assert h.registry.lookup("0001").jumper_name == "billy"  # type: ignore[union-attr]
        types = [e.type for e in h.events()]
        assert types == ["owners.reloaded", "owners.error", "owners.reloaded"]
