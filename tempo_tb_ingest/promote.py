"""Promotion: formation grouping and test-data case generation (design §3.11).

Propose-and-confirm: compute a complete promotion proposal from unpromoted
staged sessions, display it, apply only on operator confirmation. Grouping is
mechanical — exit-time window plus a GPS proximity cross-check — and every
ambiguity (missing/multiple load organizer, no-exit sessions, unmapped
sessions) is flagged in the proposal, never silently resolved.
"""

import json
import re
import shutil
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

from tempo_tb_ingest.config import Config
from tempo_tb_ingest.flightinfo import FlightInfo, haversine_m, parse_flight_log
from tempo_tb_ingest.owners import OwnersRegistry
from tempo_tb_ingest.store import Store, StoredSession

CASE_DIR_RE = re.compile(r"^(\d+)-")


@dataclass(frozen=True)
class Enriched:
    session: StoredSession
    info: FlightInfo

    @property
    def label(self) -> str:
        return f"{self.session.device_id}/{self.session.session_key}"


@dataclass
class CaseProposal:
    dirname: str  # e.g. "06-formation-20260705-2way"
    jumpers: dict[str, Enriched]  # jumper name -> session
    base_jumper: str
    is_solo: bool
    metadata: dict[str, object]
    flags: list[str] = field(default_factory=list)


@dataclass
class Proposal:
    cases: list[CaseProposal]
    no_exit: list[Enriched]  # listed for the operator, never auto-grouped
    unmapped: list[StoredSession]  # jumper unknown: fix registry, --reattribute
    flags: list[str] = field(default_factory=list)  # proposal-wide warnings


# --------------------------------------------------------------------------- #
# enrichment & grouping


def session_key_date(session_key: str) -> date | None:
    """``20260206/D243913E`` → date(2026, 2, 6)."""
    try:
        return datetime.strptime(session_key.split("/", 1)[0], "%Y%m%d").date()
    except ValueError:
        return None


def enrich_one(session: StoredSession) -> Enriched:
    # session-key date anchors logs without $GNRMC (firmware V110)
    info = parse_flight_log(
        Path(session.path), fallback_date=session_key_date(session.session_key)
    )
    return Enriched(session=session, info=info)


def enrich(sessions: list[StoredSession]) -> list[Enriched]:
    return [enrich_one(s) for s in sessions]


def group_formations(
    enriched: list[Enriched],
    *,
    exit_window_s: float,
    gps_max_separation_m: float,
) -> list[list[Enriched]]:
    """Cluster sessions whose exits are close in time AND space.

    Time: chain clustering — consecutive exits within ``exit_window_s`` join
    a candidate group. Space: within each candidate, connected components
    over pairwise exit-position distance ≤ ``gps_max_separation_m`` (sessions
    without a position stay grouped by time alone, flagged later).
    """
    with_exit = sorted(
        (e for e in enriched if e.info.exit_utc is not None),
        key=lambda e: e.info.exit_utc,  # type: ignore[arg-type, return-value]
    )
    time_groups: list[list[Enriched]] = []
    for entry in with_exit:
        if (
            time_groups
            and entry.info.exit_utc is not None
            and time_groups[-1][-1].info.exit_utc is not None
            and (entry.info.exit_utc - time_groups[-1][-1].info.exit_utc).total_seconds()
            <= exit_window_s
        ):
            time_groups[-1].append(entry)
        else:
            time_groups.append([entry])

    groups: list[list[Enriched]] = []
    for candidate in time_groups:
        groups.extend(_split_by_gps(candidate, gps_max_separation_m))
    return groups


def _split_by_gps(candidate: list[Enriched], max_m: float) -> list[list[Enriched]]:
    if len(candidate) <= 1:
        return [candidate]

    def near(a: Enriched, b: Enriched) -> bool:
        if None in (
            a.info.exit_lat_deg,
            a.info.exit_lon_deg,
            b.info.exit_lat_deg,
            b.info.exit_lon_deg,
        ):
            return True  # no position: cannot refute the time grouping
        return (
            haversine_m(
                a.info.exit_lat_deg,  # type: ignore[arg-type]
                a.info.exit_lon_deg,  # type: ignore[arg-type]
                b.info.exit_lat_deg,  # type: ignore[arg-type]
                b.info.exit_lon_deg,  # type: ignore[arg-type]
            )
            <= max_m
        )

    # connected components
    remaining = list(candidate)
    components: list[list[Enriched]] = []
    while remaining:
        component = [remaining.pop(0)]
        changed = True
        while changed:
            changed = False
            for other in list(remaining):
                if any(near(other, member) for member in component):
                    component.append(other)
                    remaining.remove(other)
                    changed = True
        components.append(sorted(component, key=lambda e: e.info.exit_utc))  # type: ignore[arg-type, return-value]
    components.sort(key=lambda c: c[0].info.exit_utc)  # type: ignore[arg-type, return-value]
    return components


# --------------------------------------------------------------------------- #
# proposal


def next_case_number(test_data_root: Path) -> int:
    numbers = [0]
    if test_data_root.is_dir():
        for entry in test_data_root.iterdir():
            match = CASE_DIR_RE.match(entry.name)
            if entry.is_dir() and match:
                numbers.append(int(match.group(1)))
    return max(numbers) + 1


def build_proposal(store: Store, config: Config) -> Proposal:
    unpromoted = [s for s in store.sessions() if s.promoted_to is None]
    unmapped = [s for s in unpromoted if s.jumper is None]
    mapped = [s for s in unpromoted if s.jumper is not None]

    enriched = enrich(mapped)
    no_exit = [e for e in enriched if e.info.exit_utc is None]
    groups = group_formations(
        [e for e in enriched if e.info.exit_utc is not None],
        exit_window_s=config.promote.exit_window_s,
        gps_max_separation_m=config.promote.gps_max_separation_m,
    )

    proposal = Proposal(cases=[], no_exit=no_exit, unmapped=unmapped)
    number = next_case_number(config.promote.test_data_root)
    for group in groups:
        case = _build_case(group, number, config)
        proposal.cases.append(case)
        number += 1
    if unmapped:
        proposal.flags.append(
            f"{len(unmapped)} session(s) unmapped in device-owners.json — fix the registry "
            "and run --reattribute"
        )
    proposal.flags.extend(_same_takeoff_hints(proposal.cases, no_exit))
    return proposal


#: two logs whose first GNSS fixes are this close rode the same airplane
TAKEOFF_WINDOW_S = 90.0


def _same_takeoff_hints(cases: list[CaseProposal], no_exit: list[Enriched]) -> list[str]:
    """Surface probable grouping misses (issue #1, 2026-07-10).

    Logs that share a takeoff (first GNSS fix within TAKEOFF_WINDOW_S) but were
    not grouped mean one of the exits is wrong or missing — the operator should
    look before accepting the proposal. Detection quality is upstream (firmware);
    this makes the symptom visible instead of silent.
    """
    hints: list[str] = []

    def first_fix(entry: Enriched) -> datetime | None:
        return entry.info.first_fix_utc

    def close(a: datetime | None, b: datetime | None) -> bool:
        return a is not None and b is not None and abs((a - b).total_seconds()) <= TAKEOFF_WINDOW_S

    for i, case_a in enumerate(cases):
        for case_b in cases[i + 1 :]:
            pairs = [
                (ea, eb)
                for ea in case_a.jumpers.values()
                for eb in case_b.jumpers.values()
                if close(first_fix(ea), first_fix(eb))
            ]
            if pairs:
                ea, eb = pairs[0]
                delta = abs(
                    (ea.info.exit_utc - eb.info.exit_utc).total_seconds()  # type: ignore[operator]
                )
                hints.append(
                    f"{case_a.dirname} and {case_b.dirname} share a takeoff "
                    f"(first fixes within {TAKEOFF_WINDOW_S:.0f}s) but exits differ by "
                    f"{delta / 60:.1f} min — one exit detection is likely wrong; "
                    "review before applying"
                )
    for entry in no_exit:
        for case in cases:
            if any(close(first_fix(entry), first_fix(e)) for e in case.jumpers.values()):
                hints.append(
                    f"no-exit session {entry.label} shares a takeoff with {case.dirname} — "
                    "possibly a jump whose exit went undetected"
                )
                break
    return hints


def _build_case(group: list[Enriched], number: int, config: Config) -> CaseProposal:
    flags: list[str] = []
    jumpers: dict[str, Enriched] = {}
    for entry in group:
        jumper = entry.session.jumper
        assert jumper is not None  # unmapped sessions never reach grouping
        if jumper in jumpers:
            flags.append(
                f"jumper '{jumper}' has two sessions in this group "
                f"({jumpers[jumper].label} and {entry.label}) — keeping the first, "
                "the duplicate needs manual handling"
            )
            continue
        jumpers[jumper] = entry

    is_solo = len(jumpers) == 1
    date_str = group[0].info.date.strftime("%Y%m%d") if group[0].info.date else "undated"
    exit_utc = group[0].info.exit_utc

    organizers = [j for j, e in jumpers.items() if e.session.jumper_is_lo]
    if len(organizers) == 1:
        base = organizers[0]
    elif not organizers:
        base = sorted(jumpers)[0]
        if not is_solo:
            flags.append(f"no load organizer in group — baseJumper defaulted to '{base}'")
    else:
        base = organizers[0]
        flags.append(
            f"multiple load organizers in group ({', '.join(organizers)}) — "
            f"baseJumper defaulted to '{base}'"
        )

    if is_solo:
        only = next(iter(jumpers))
        slug = f"solo-{only}-{date_str}"
        title = f"Solo — {only}"
    else:
        slug = f"formation-{date_str}-{len(jumpers)}way"
        title = f"{len(jumpers)}-Way Formation"
    dirname = f"{number:02d}-{slug}"

    when = exit_utc.strftime("%Y-%m-%d %H:%M UTC") if exit_utc else f"{date_str} (no exit time)"
    session_ids = ", ".join(
        f"{e.session.session_key.split('/')[1]} ({j})" for j, e in sorted(jumpers.items())
    )
    exits = "; ".join(
        f"{j}: {e.info.exit_utc.strftime('%H:%M:%S.%f')[:-3]}Z ({e.info.exit_source})"
        for j, e in sorted(jumpers.items())
        if e.info.exit_utc is not None
    )
    metadata: dict[str, object] = {
        "name": f"{title} — {when}",
        "description": (
            f"Auto-ingested by tempo-tb-ingest. Jumpers: {', '.join(sorted(jumpers))}. "
            f"Exits: {exits}. Session IDs: {session_ids}."
        ),
        "dropzone": {
            "name": config.dropzone.name,
            "lat_deg": config.dropzone.lat_deg,
            "lon_deg": config.dropzone.lon_deg,
            "elevation_m": config.dropzone.elevation_m,
            "timezone": config.dropzone.timezone,
        },
        "jumpers": sorted(jumpers),
        "baseJumper": base,
        "isSolo": is_solo,
        "tags": sorted(
            {"auto-ingested", date_str}
            | ({"solo"} if is_solo else {"formation", f"{len(jumpers)}way"})
        ),
    }
    return CaseProposal(
        dirname=dirname,
        jumpers=jumpers,
        base_jumper=base,
        is_solo=is_solo,
        metadata=metadata,
        flags=flags,
    )


# --------------------------------------------------------------------------- #
# apply


def apply_proposal(proposal: Proposal, store: Store, config: Config) -> list[Path]:
    """Copy files + write metadata; staging remains intact. Returns case dirs."""
    created: list[Path] = []
    for case in proposal.cases:
        case_dir = config.promote.test_data_root / case.dirname
        if case_dir.exists():
            raise FileExistsError(
                f"{case_dir} already exists — re-run promote to renumber against "
                "the current test-data tree"
            )
        case_dir.mkdir(parents=True)
        (case_dir / "metadata.json").write_text(
            json.dumps(case.metadata, indent=2) + "\n", encoding="utf-8"
        )
        for jumper, entry in case.jumpers.items():
            jumper_dir = case_dir / jumper
            jumper_dir.mkdir()
            shutil.copyfile(entry.session.path, jumper_dir / "flight.txt")
            store.mark_promoted(
                entry.session.device_id,
                entry.session.session_key,
                f"{case.dirname}/{jumper}",
            )
        created.append(case_dir)
    return created


def reattribute(store: Store, registry: OwnersRegistry) -> int:
    """Re-bind still-unpromoted, unmapped sessions from the current registry."""
    updated = 0
    for session in store.sessions():
        if session.promoted_to is not None or session.jumper is not None:
            continue
        owner = registry.lookup(session.device_id)
        if owner is None:
            continue
        store.update_attribution(
            session.device_id,
            session.session_key,
            owner.jumper_name,
            owner.is_load_organizer,
        )
        updated += 1
    return updated


# --------------------------------------------------------------------------- #
# rendering (CLI display)


def render_proposal(proposal: Proposal) -> str:
    lines: list[str] = []
    if not proposal.cases and not proposal.no_exit and not proposal.unmapped:
        return "Nothing to promote: no unpromoted sessions in the staging index.\n"
    for case in proposal.cases:
        lines.append(f"CASE {case.dirname}")
        lines.append(f"  {case.metadata['name']}")
        for jumper, entry in sorted(case.jumpers.items()):
            marker = " (base)" if jumper == case.base_jumper else ""
            exit_s = (
                entry.info.exit_utc.strftime("%H:%M:%S.%f")[:-3] + "Z"
                if entry.info.exit_utc
                else "no exit"
            )
            lines.append(f"    {jumper}{marker}: {entry.label}  exit {exit_s}")
        for flag in case.flags:
            lines.append(f"  ⚠ {flag}")
        lines.append("")
    if proposal.no_exit:
        lines.append("NOT PROPOSED (no detected exit — ground test / non-jump?):")
        for entry in proposal.no_exit:
            minutes = (entry.info.duration_s or 0) / 60
            lines.append(f"    {entry.label}  ({minutes:.1f} min, jumper {entry.session.jumper})")
        lines.append("")
    if proposal.unmapped:
        lines.append("UNMAPPED (no device-owners.json entry at harvest time):")
        for session in proposal.unmapped:
            lines.append(f"    {session.device_id}/{session.session_key}")
        lines.append("")
    for flag in proposal.flags:
        lines.append(f"⚠ {flag}")
    return "\n".join(lines) + "\n"
