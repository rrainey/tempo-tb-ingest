"""Step 7: destructive hardware tier — real interrupted transfer + resume.

Runs ONLY under ``-m destructive`` (``make destructive``) and ONLY against a
device whose SD card carries the ``/SD:/testok`` marker (CLAUDE.md); the tier
hard-fails without it. The interruption is a host-side link kill
(``bluetoothctl disconnect``) — characterized 2026-07-08 as raising
``smpclient.transport.SMPTransportDisconnected`` with a byte-exact partial
sink, the same surface as a device leaving range (supervision timeout).
"""

import asyncio
import hashlib
import io
import os
import subprocess
from collections.abc import AsyncIterator

import pytest

from tempo_tb_ingest.device.protocol import LinkDisconnected, log_path
from tempo_tb_ingest.device.smp_link import SmpLink, connect_with_retry

pytestmark = pytest.mark.destructive

DEV_DEVICE = os.environ.get("TEMPO_DEV_DEVICE", "Tempo-BT-0010")
MIN_INTERRUPTIBLE_SIZE = 200 * 1024  # need a window long enough to kill mid-air


async def fresh_link() -> SmpLink:
    link = SmpLink(DEV_DEVICE, connect_timeout_s=15.0)
    await connect_with_retry(link, attempts=5, backoff_s=3.0)
    return link


def bluez_mac(name: str) -> str:
    out = subprocess.run(
        ["bluetoothctl", "devices"], capture_output=True, text=True, timeout=15
    ).stdout
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[2] == name:
            return parts[1]
    pytest.fail(f"{name} not in bluetoothctl devices; scan first")


@pytest.fixture
async def guarded_link() -> AsyncIterator[SmpLink]:
    """A connected link, hard-gated on the testok marker."""
    link = await fresh_link()
    if not await link.probe_testok():
        await link.disconnect()
        pytest.fail(
            f"{DEV_DEVICE}: /SD:/testok marker ABSENT — refusing destructive tier "
            "(production card protection, see CLAUDE.md)"
        )
    yield link
    await link.disconnect()


async def pick_target(link: SmpLink) -> tuple[str, int]:
    result = await link.session_list()
    if result.truncated:
        pytest.fail("session list truncated on dev card; trim it")
    sized = []
    for key in result.keys:
        try:
            sized.append((key, await link.read_size(log_path(key))))
        except Exception:
            continue
    big = [(k, s) for k, s in sized if s >= MIN_INTERRUPTIBLE_SIZE]
    if not big:
        pytest.fail(
            f"no session file >= {MIN_INTERRUPTIBLE_SIZE} bytes on the testok card; "
            "copy a multi-MB flight.txt into /logs/<date>/<8hex>/ first"
        )
    return max(big, key=lambda pair: pair[1])


class TestInterruptedTransferResume:
    async def test_kill_mid_download_then_resume_byte_identical(
        self, guarded_link: SmpLink
    ) -> None:
        link = guarded_link
        mac = bluez_mac(DEV_DEVICE)
        key, size = await pick_target(link)
        path = log_path(key)

        # baseline (uninterrupted)
        baseline_sink = io.BytesIO()
        await link.download(path, baseline_sink)
        baseline = baseline_sink.getvalue()
        assert len(baseline) == size
        baseline_sha = hashlib.sha256(baseline).hexdigest()

        # interrupted: kill the link at ~35%
        partial_sink = io.BytesIO()
        kill_trigger = asyncio.Event()

        def on_progress(done: int) -> None:
            if done > size * 0.35:
                kill_trigger.set()

        async def killer() -> None:
            await kill_trigger.wait()
            await asyncio.to_thread(
                subprocess.run,
                ["bluetoothctl", "disconnect", mac],
                capture_output=True,
                timeout=15,
            )

        killer_task = asyncio.create_task(killer())
        with pytest.raises(LinkDisconnected):
            await link.download(path, partial_sink, progress=on_progress)
        await killer_task
        # clean up the killed client before opening a fresh connection —
        # a dead-but-undisconnected BleakClient breaks subsequent D-Bus use
        await link.disconnect()

        partial = partial_sink.getvalue()
        assert 0 < len(partial) < size, "expected a genuine partial transfer"
        assert partial == baseline[: len(partial)], "partial sink must be a byte-exact prefix"

        # resume on a fresh connection completes to byte identity
        await asyncio.sleep(3)  # allow re-advertising
        resumed = await fresh_link()
        try:
            tail_sink = io.BytesIO()
            written = await resumed.download(path, tail_sink, offset=len(partial))
            assert written == size - len(partial)
            combined = partial + tail_sink.getvalue()
            assert hashlib.sha256(combined).hexdigest() == baseline_sha
        finally:
            await resumed.disconnect()
