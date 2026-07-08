"""Flight-log enrichment: date, duration, precise exit UTC, exit position.

A faithful Python port of ``tempo-testbed/scripts/flight-info.sh`` (the
reference implementation), extended with the exit GPS position needed for
formation-grouping's proximity cross-check (design §3.11).

Exit algorithm (as in the script):
1. Find the device-ms timestamp of the exit event — the first
   ``$PST … JUMPED`` transition, or the first of ≥10 consecutive ``$PIMU``
   samples below 0.8 g (whichever occurred *earlier*).
2. Take the UTC time of the most recent preceding ``$GNGGA`` and the
   device-ms of the ``$PTH`` record paired with it (``$PTH`` immediately
   follows each GGA and carries the millis() of that GGA's arrival, per
   LOG-FORMAT.md).
3. ``exit_utc = gga_utc + (event_ms - pth_ms)``, clamped to the day.

Format-era notes (reviewed against Dropkick LOG-FORMAT.md 55/155, 2026-07-08):

- ``$PST`` matching is **Tempo-only by decision**: the 5-field transition
  form (``$PST,<ms>,<from>,<to>,<reason>``) with the ``JUMPED`` state, as
  the reference script matches. Dropkick-era 3-field records
  (``$PST,<ms>,JUMPING``) are deliberately not matched — a hand-staged
  Dropkick log still gets an exit via the accel fallback.
- Firmware V110 logs carry no ``$GNRMC`` (hence ``fallback_date``); the
  Dropkick-era doc likewise documents only GGA/GLL.
- Known, accepted imprecision shared with the reference: an event landing
  between a GGA and its PTH pairs with the previous epoch (≤ ~1 s), and
  ``pth_ms`` marks sentence *arrival* (receiver latency biases all exits
  slightly early — cancels in cross-device comparisons).

The parser reads the interleaved extended-NMEA text log line by line; it is
tolerant of checksums and malformed lines (skipped, counted).
"""

import math
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

LOW_G_THRESHOLD = 9.81 * 0.8
LOW_G_CONSECUTIVE = 10

_RMC_RE = re.compile(r"^\$G[NP]RMC,")
_GGA_RE = re.compile(r"^\$G[NP]GGA,")


@dataclass(frozen=True)
class FlightInfo:
    """Everything promote needs to identify and group one session's jump."""

    date: date | None
    first_fix_utc: datetime | None
    last_fix_utc: datetime | None
    duration_s: float | None
    exit_utc: datetime | None
    exit_source: str | None  # "PST" | "ACCEL" | None
    exit_lat_deg: float | None
    exit_lon_deg: float | None
    gga_count: int
    bad_lines: int


def _field(fields: list[str], index: int) -> str:
    """NMEA field, checksum-stripped; '' if absent."""
    if index >= len(fields):
        return ""
    return fields[index].split("*", 1)[0]


def _parse_nmea_time(raw: str) -> float | None:
    """HHMMSS.SS → seconds of day."""
    if len(raw) < 6:
        return None
    try:
        return int(raw[0:2]) * 3600 + int(raw[2:4]) * 60 + float(raw[4:])
    except ValueError:
        return None


def _parse_nmea_date(raw: str) -> date | None:
    """DDMMYY → date (century 2000, as the reference script assumes)."""
    if len(raw) != 6 or not raw.isdigit():
        return None
    try:
        return date(2000 + int(raw[4:6]), int(raw[2:4]), int(raw[0:2]))
    except ValueError:
        return None


def _parse_latlon(lat: str, ns: str, lon: str, ew: str) -> tuple[float, float] | None:
    """ddmm.mmmm/dddmm.mmmm + hemisphere → signed degrees."""
    try:
        lat_deg = int(lat[0:2]) + float(lat[2:]) / 60.0
        lon_deg = int(lon[0:3]) + float(lon[3:]) / 60.0
    except (ValueError, IndexError):
        return None
    if ns == "S":
        lat_deg = -lat_deg
    if ew == "W":
        lon_deg = -lon_deg
    return (lat_deg, lon_deg)


@dataclass(frozen=True)
class _GGAContext:
    time_s: float
    lat: float | None
    lon: float | None


@dataclass(frozen=True)
class _EventCapture:
    """Snapshot of (last GGA, last PTH) taken at the event line, exactly as
    the reference awk uses its ``last_gga_time`` / ``last_pth_ms``."""

    event_ms: float
    gga: _GGAContext
    pth_ms: float | None


def parse_flight_log(path: Path, *, fallback_date: date | None = None) -> FlightInfo:
    """Parse one flight log.

    ``fallback_date`` anchors UTC timestamps when the log carries no
    ``$GNRMC`` records (observed: firmware V110 logs have none) — promote
    passes the session-key date (``20260206/...``), which the firmware
    derived from GNSS when it created the session directory.
    """
    gnss_date: date | None = None
    first_fix_s: float | None = None
    last_fix_s: float | None = None
    gga_count = 0
    bad_lines = 0

    last_gga: _GGAContext | None = None
    last_pth_ms: float | None = None

    pst_event: _EventCapture | None = None
    accel_event: _EventCapture | None = None
    low_g_count = 0
    low_g_first: _EventCapture | None = None

    with path.open(encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line.startswith("$"):
                if line:
                    bad_lines += 1
                continue
            fields = line.split(",")
            tag = fields[0]

            if gnss_date is None and _RMC_RE.match(line):
                gnss_date = _parse_nmea_date(_field(fields, 9))

            elif _GGA_RE.match(line):
                t = _parse_nmea_time(_field(fields, 1))
                if t is None:
                    bad_lines += 1
                    continue
                gga_count += 1
                if first_fix_s is None:
                    first_fix_s = t
                last_fix_s = t
                pos = _parse_latlon(
                    _field(fields, 2), _field(fields, 3), _field(fields, 4), _field(fields, 5)
                )
                last_gga = _GGAContext(
                    time_s=t,
                    lat=pos[0] if pos else None,
                    lon=pos[1] if pos else None,
                )

            elif tag == "$PTH":
                try:
                    last_pth_ms = float(_field(fields, 1))
                except ValueError:
                    bad_lines += 1

            elif tag == "$PST" and pst_event is None:
                to_state = _field(fields, 3)
                if to_state.startswith("JUMPED") and last_gga is not None:
                    try:
                        pst_event = _EventCapture(
                            event_ms=float(_field(fields, 1)), gga=last_gga, pth_ms=last_pth_ms
                        )
                    except ValueError:
                        bad_lines += 1

            elif tag == "$PIMU" and accel_event is None:
                try:
                    ms = float(_field(fields, 1))
                    ax = float(_field(fields, 2))
                    ay = float(_field(fields, 3))
                    az = float(_field(fields, 4))
                except ValueError:
                    bad_lines += 1
                    continue
                if math.sqrt(ax * ax + ay * ay + az * az) < LOW_G_THRESHOLD:
                    low_g_count += 1
                    if low_g_count == 1 and last_gga is not None:
                        low_g_first = _EventCapture(event_ms=ms, gga=last_gga, pth_ms=last_pth_ms)
                    if low_g_count >= LOW_G_CONSECUTIVE and low_g_first is not None:
                        accel_event = low_g_first
                else:
                    low_g_count = 0
                    low_g_first = None

    if gnss_date is None:
        gnss_date = fallback_date

    # choose whichever event occurred first (device-ms), like the script
    event: _EventCapture | None
    source: str | None
    if pst_event and accel_event:
        event, source = (
            (accel_event, "ACCEL")
            if accel_event.event_ms <= pst_event.event_ms
            else (pst_event, "PST")
        )
    elif pst_event:
        event, source = pst_event, "PST"
    elif accel_event:
        event, source = accel_event, "ACCEL"
    else:
        event, source = None, None

    exit_utc: datetime | None = None
    exit_lat: float | None = None
    exit_lon: float | None = None
    if event is not None and gnss_date is not None and event.pth_ms is not None:
        total_s = event.gga.time_s + (event.event_ms - event.pth_ms) / 1000.0
        day_shift, total_s = divmod(total_s, 86400.0)
        exit_utc = datetime.combine(gnss_date, time(0), tzinfo=UTC) + timedelta(
            days=day_shift, seconds=total_s
        )
        exit_lat, exit_lon = event.gga.lat, event.gga.lon

    duration_s: float | None = None
    if first_fix_s is not None and last_fix_s is not None:
        duration_s = last_fix_s - first_fix_s
        if duration_s < 0:
            duration_s += 86400.0

    def fix_dt(seconds: float | None) -> datetime | None:
        if seconds is None or gnss_date is None:
            return None
        return datetime.combine(gnss_date, time(0), tzinfo=UTC) + timedelta(seconds=seconds)

    return FlightInfo(
        date=gnss_date,
        first_fix_utc=fix_dt(first_fix_s),
        last_fix_utc=fix_dt(last_fix_s),
        duration_s=duration_s,
        exit_utc=exit_utc,
        exit_source=source,
        exit_lat_deg=exit_lat,
        exit_lon_deg=exit_lon,
        gga_count=gga_count,
        bad_lines=bad_lines,
    )


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters."""
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))
