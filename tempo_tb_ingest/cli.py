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
    import asyncio
    import logging
    import signal

    from tempo_tb_ingest.config import Config, ConfigError
    from tempo_tb_ingest.daemon import AlreadyRunning, Daemon

    try:
        cfg = Config.load(config)
    except ConfigError as exc:
        typer.echo(f"daemon: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    logging.basicConfig(
        level=getattr(logging, cfg.log.level.upper()),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    async def run() -> None:
        instance = Daemon(cfg)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, instance.stop, f"signal {sig.name}")
        typer.echo(f"daemon: serving on http://{cfg.http.listen}")
        await instance.run()

    try:
        asyncio.run(run())
    except AlreadyRunning as exc:
        typer.echo(f"daemon: {exc}", err=True)
        raise typer.Exit(code=3) from exc
    except OSError as exc:
        typer.echo(f"daemon: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@app.command()
def promote(
    config: Annotated[Path | None, typer.Option(help="TOML config file")] = None,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Apply without confirmation")] = False,
    reattribute: Annotated[
        bool,
        typer.Option("--reattribute", help="Re-bind unmapped sessions from device-owners.json"),
    ] = False,
) -> None:
    """Group staged sessions into test-data analysis cases (propose-and-confirm)."""
    from tempo_tb_ingest import promote as promote_mod
    from tempo_tb_ingest.config import Config, ConfigError
    from tempo_tb_ingest.owners import OwnersRegistry
    from tempo_tb_ingest.store import Store

    try:
        cfg = Config.load(config)
    except ConfigError as exc:
        typer.echo(f"promote: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    try:
        store = Store(
            staging_root=cfg.store.staging_root,
            data_dir=cfg.store.data_dir,
            spool_dir=cfg.harvest.spool_dir,
        )
    except OSError as exc:
        typer.echo(f"promote: cannot open store: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    try:
        if reattribute:
            registry = OwnersRegistry(cfg.store.resolved_owners_file())
            updated = promote_mod.reattribute(store, registry)
            typer.echo(f"reattributed {updated} session(s)")

        proposal = promote_mod.build_proposal(store, cfg)
        typer.echo(promote_mod.render_proposal(proposal))
        if not proposal.cases:
            return
        if not yes and not typer.confirm(f"Apply {len(proposal.cases)} case(s) to test-data?"):
            typer.echo("aborted; nothing applied")
            raise typer.Exit(code=1)
        created = promote_mod.apply_proposal(proposal, store, cfg)
        for case_dir in created:
            typer.echo(f"created {case_dir}")
    finally:
        store.close()


@app.command(name="rebuild-index")
def rebuild_index(
    config: Annotated[Path | None, typer.Option(help="TOML config file")] = None,
    mark_baseline: Annotated[
        bool,
        typer.Option(
            "--mark-baseline",
            help="After rebuilding, mark every indexed session as already promoted"
            " ('pre-existing') so promote only proposes future harvests.",
        ),
    ] = False,
    except_date: Annotated[
        list[str],
        typer.Option(
            "--except-date",
            help="With --mark-baseline: leave sessions of this YYYYMMDD unmarked (repeatable).",
        ),
    ] = [],  # noqa: B006 - typer needs a literal default
) -> None:
    """Reconstruct the session index by walking the staging tree (design §3.6)."""
    from tempo_tb_ingest.config import Config, ConfigError
    from tempo_tb_ingest.store import Store

    try:
        cfg = Config.load(config)
    except ConfigError as exc:
        typer.echo(f"rebuild-index: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    store = Store(
        staging_root=cfg.store.staging_root,
        data_dir=cfg.store.data_dir,
        spool_dir=cfg.harvest.spool_dir,
    )
    try:
        count = store.rebuild_index()
        typer.echo(f"indexed {count} session(s) from {cfg.store.staging_root}")
        if mark_baseline:
            marked = 0
            for session in store.sessions():
                if session.promoted_to is not None:
                    continue
                if session.session_key.split("/", 1)[0] in except_date:
                    continue
                store.mark_promoted(session.device_id, session.session_key, "pre-existing")
                marked += 1
            typer.echo(f"marked {marked} session(s) as pre-existing baseline")
    finally:
        store.close()


@app.command()
def replay(
    recording: Annotated[Path, typer.Argument(help="JSONL event recording")],
    speed: Annotated[float, typer.Option(help="Playback speed multiplier")] = 1.0,
    loop: Annotated[bool, typer.Option(help="Restart at end of file")] = False,
    listen: Annotated[
        str | None,
        typer.Option(help="Serve the API/dashboard from the replay, e.g. 127.0.0.1:8080"),
    ] = None,
    static: Annotated[
        Path | None,
        typer.Option(help="Static dashboard build to serve at / (e.g. dashboard/dist)"),
    ] = None,
) -> None:
    """Re-publish a recorded event stream (console, and optionally the API)."""
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
        runner = None
        fold_task = None

        if listen is not None:
            from tempo_tb_ingest.api import create_app, serve
            from tempo_tb_ingest.statefold import StateFold

            fold = StateFold()
            fold_subscription = bus.subscribe()

            async def fold_pump() -> None:
                async for env in fold_subscription:
                    fold.apply(env)

            fold_task = asyncio.create_task(fold_pump())
            host, _, port = listen.rpartition(":")
            app_ = create_app(bus, fold.snapshot, static_dir=static)
            runner = await serve(app_, host or "127.0.0.1", int(port))
            typer.echo(f"replay: serving API on http://{listen}")

        async def printer() -> None:
            async for env in subscription:
                typer.echo(
                    f"{env.ts.strftime('%H:%M:%S.%f')[:-3]}  {env.seq:>6}  "
                    f"{env.type:<28}  {json.dumps(env.data)}"
                )

        printer_task = asyncio.create_task(printer())
        try:
            stats = await replay_events(recording, bus, speed=speed, loop=loop)
            if listen is not None:
                typer.echo("replay: finished; serving final state (Ctrl-C to exit)")
                await asyncio.Event().wait()
        finally:
            bus.close()
            await printer_task
            if fold_task is not None:
                await fold_task
            if runner is not None:
                await runner.cleanup()
        if stats.skipped:
            typer.echo(f"replay: skipped {stats.skipped} malformed line(s)", err=True)

    asyncio.run(run())


def main() -> None:
    """Console-script entry point."""
    app()
