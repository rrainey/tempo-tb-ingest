"""Event-stream recorder and replay (design §3.8).

Recording: every envelope appended as one JSON line to
``<directory>/YYYYMMDD.jsonl``, the day taken from the envelope's UTC
timestamp — so a recording replayed or post-processed later lands in the same
files regardless of wall clock.

Replay: read a JSONL recording and publish the envelopes verbatim onto a bus,
pacing by the recorded inter-event intervals divided by ``speed``. Malformed
lines are skipped loudly (logged + counted), never silently.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO

from tempo_tb_ingest.events import Envelope, EventBus, EventError, Subscription

logger = logging.getLogger(__name__)


class Recorder:
    """Subscribes to a bus and appends every envelope to daily JSONL files."""

    def __init__(self, bus: EventBus, directory: Path) -> None:
        self._directory = directory
        self._subscription: Subscription = bus.subscribe()
        self._open_day: str | None = None
        self._open_file: TextIO | None = None
        self.lines_written = 0

    def path_for(self, env: Envelope) -> Path:
        return self._directory / (env.ts.strftime("%Y%m%d") + ".jsonl")

    def _file_for(self, env: Envelope) -> TextIO:
        day = env.ts.strftime("%Y%m%d")
        if self._open_file is None or self._open_day != day:
            if self._open_file is not None:
                self._open_file.close()
            self._directory.mkdir(parents=True, exist_ok=True)
            self._open_file = self.path_for(env).open("a", encoding="utf-8")
            self._open_day = day
        return self._open_file

    def write(self, env: Envelope) -> None:
        fh = self._file_for(env)
        fh.write(env.to_json() + "\n")
        fh.flush()
        self.lines_written += 1

    async def run(self) -> None:
        """Consume the subscription until the bus closes it."""
        try:
            async for env in self._subscription:
                self.write(env)
        finally:
            self.close()

    def close(self) -> None:
        if self._open_file is not None:
            self._open_file.close()
            self._open_file = None
            self._open_day = None


@dataclass
class ReplayStats:
    published: int = 0
    skipped: int = 0
    cycles: int = 0
    skipped_lines: list[int] = field(default_factory=list)  # 1-based line numbers


def load_recording(path: Path) -> tuple[list[Envelope], ReplayStats]:
    """Parse a JSONL recording; malformed lines are skipped loudly."""
    stats = ReplayStats()
    envelopes: list[Envelope] = []
    with path.open(encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            if not line.strip():
                continue
            try:
                envelopes.append(Envelope.from_json(line))
            except EventError as exc:
                stats.skipped += 1
                stats.skipped_lines.append(lineno)
                logger.warning("%s:%d: skipping malformed event line: %s", path, lineno, exc)
    return envelopes, stats


async def replay(
    path: Path,
    bus: EventBus,
    *,
    speed: float = 1.0,
    loop: bool = False,
    max_cycles: int | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> ReplayStats:
    """Publish a recording onto ``bus``, pacing by recorded timestamps.

    ``speed`` is a multiplier (10 = ten times faster than recorded). ``loop``
    repeats forever (or ``max_cycles`` times); between cycles there is no
    artificial delay — clients see a fresh pass immediately.
    """
    if speed <= 0:
        raise ValueError("speed must be > 0")
    envelopes, stats = load_recording(path)
    if not envelopes:
        raise EventError(f"{path}: no valid events to replay")

    cycles = max_cycles if max_cycles is not None else (None if loop else 1)
    max_seq = max(env.seq for env in envelopes)
    seq_offset = 0
    while True:
        if stats.cycles > 0:
            # Loop restart = a fresh daemon run: re-sequence monotonically and
            # publish the restart marker so folds reset instead of dropping
            # repeated seqs / double-counting (bug found in the looping demo,
            # 2026-07-10).
            seq_offset += max_seq + 1
            bus.publish_envelope(
                Envelope(
                    seq=seq_offset,
                    ts=envelopes[0].ts,
                    type="daemon.started",
                    data={"version": "replay-loop", "config": {}},
                )
            )
        previous = None
        for env in envelopes:
            if previous is not None:
                gap = (env.ts - previous).total_seconds()
                if gap > 0:
                    await sleep(gap / speed)
            out = env.model_copy(update={"seq": env.seq + seq_offset}) if seq_offset else env
            bus.publish_envelope(out)
            stats.published += 1
            previous = env.ts
        stats.cycles += 1
        if cycles is not None and stats.cycles >= cycles:
            return stats
