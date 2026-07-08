"""Command-line interface.

Commands are wired in as their modules land (docs/implementation-plan.md):
`daemon` (step 15), `promote` (step 13), `replay` (step 4), `probe` (step 6).
"""

from pathlib import Path
from typing import Annotated

import typer

from tempo_tb_ingest import __version__

app = typer.Typer(
    name="tempo-tb-ingest",
    help="Automated BLE harvesting of Tempo-BT skydiving logs.",
    no_args_is_help=True,
)

_NOT_YET = "not implemented yet — see docs/implementation-plan.md"


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"tempo-tb-ingest {__version__}")
        raise typer.Exit()


@app.callback()
def _root(
    version: Annotated[
        bool,
        typer.Option("--version", callback=_version_callback, is_eager=True),
    ] = False,
) -> None:
    """Automated BLE harvesting of Tempo-BT skydiving logs."""


@app.command()
def daemon(
    config: Annotated[Path | None, typer.Option(help="TOML config file")] = None,
) -> None:
    """Run the ingestion daemon (scanner, return detector, harvester, API)."""
    typer.echo(f"daemon: {_NOT_YET}", err=True)
    raise typer.Exit(code=2)


@app.command()
def promote(
    config: Annotated[Path | None, typer.Option(help="TOML config file")] = None,
) -> None:
    """Group staged sessions into test-data analysis cases (propose-and-confirm)."""
    typer.echo(f"promote: {_NOT_YET}", err=True)
    raise typer.Exit(code=2)


@app.command()
def replay(
    recording: Annotated[Path, typer.Argument(help="JSONL event recording")],
    speed: Annotated[float, typer.Option(help="Playback speed multiplier")] = 1.0,
    loop: Annotated[bool, typer.Option(help="Restart at end of file")] = False,
) -> None:
    """Re-publish a recorded event stream (console output; API serving lands in step 14)."""
    import asyncio
    import json

    from tempo_tb_ingest.events import EventBus
    from tempo_tb_ingest.recorder import replay as replay_events

    if not recording.is_file():
        typer.echo(f"replay: recording not found: {recording}", err=True)
        raise typer.Exit(code=1)

    async def run() -> None:
        bus = EventBus()
        subscription = bus.subscribe()

        async def printer() -> None:
            async for env in subscription:
                typer.echo(
                    f"{env.ts.strftime('%H:%M:%S.%f')[:-3]}  {env.seq:>6}  "
                    f"{env.type:<28}  {json.dumps(env.data)}"
                )

        printer_task = asyncio.create_task(printer())
        try:
            stats = await replay_events(recording, bus, speed=speed, loop=loop)
        finally:
            bus.close()
            await printer_task
        if stats.skipped:
            typer.echo(f"replay: skipped {stats.skipped} malformed line(s)", err=True)

    asyncio.run(run())


def main() -> None:
    """Console-script entry point."""
    app()
