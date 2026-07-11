"""Adapter identity and role resolution (design §3.13, plan step 20).

Configuration names adapters by **BlueZ controller address** (stable per
dongle chip) or ``hciN`` (bench convenience; index depends on plug order —
observed: each newly attached dongle becomes the system *default* adapter).
Resolution must go through BlueZ D-Bus: the dongles' HCI-level public address
reads all-zeros, so ``hciconfig`` cannot identify them.

``resolve_roles`` maps the ``[adapter]`` config onto discovered controllers
and selects the operating mode:

- scan == the sole transfer entry → **single-adapter mode** (the validated
  v1 behavior: scanner pauses during connections);
- otherwise → **pool mode**: a dedicated scan adapter that must not appear in
  the transfer list, plus 1..N transfer adapters.
"""

import re
from dataclasses import dataclass

from tempo_tb_ingest.config import AdapterConfig, ConfigError

ADDRESS_RE = re.compile(r"^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$")
HCI_RE = re.compile(r"^hci\d+$")


@dataclass(frozen=True)
class AdapterInfo:
    hci: str  # "hci1" — what bleak's `adapter` kwarg wants
    address: str  # BlueZ controller address (uppercase)
    name: str
    powered: bool


@dataclass(frozen=True)
class ResolvedRoles:
    scan: AdapterInfo
    transfer: list[AdapterInfo]
    single_adapter_mode: bool


async def discover_adapters() -> list[AdapterInfo]:
    """Enumerate BlueZ controllers via D-Bus (org.bluez.Adapter1)."""
    from dbus_fast import BusType
    from dbus_fast.aio import MessageBus

    bus = await MessageBus(bus_type=BusType.SYSTEM).connect()
    try:
        introspection = await bus.introspect("org.bluez", "/")
        root = bus.get_proxy_object("org.bluez", "/", introspection)
        manager = root.get_interface("org.freedesktop.DBus.ObjectManager")
        objects = await manager.call_get_managed_objects()  # type: ignore[attr-defined]
    finally:
        bus.disconnect()

    adapters: list[AdapterInfo] = []
    for path, interfaces in objects.items():
        props = interfaces.get("org.bluez.Adapter1")
        if props is None:
            continue
        adapters.append(
            AdapterInfo(
                hci=path.rsplit("/", 1)[-1],
                address=str(props["Address"].value).upper(),
                name=str(props["Alias"].value) if "Alias" in props else "",
                powered=bool(props["Powered"].value) if "Powered" in props else False,
            )
        )
    adapters.sort(key=lambda a: a.hci)
    return adapters


def resolve_spec(spec: str, adapters: list[AdapterInfo]) -> AdapterInfo:
    """One config entry ('hciN' or controller address) → a discovered adapter."""
    if HCI_RE.match(spec):
        for adapter in adapters:
            if adapter.hci == spec:
                return adapter
        raise ConfigError(
            f"adapter {spec!r} not present (have: {', '.join(a.hci for a in adapters) or 'none'})"
        )
    if ADDRESS_RE.match(spec):
        wanted = spec.upper()
        for adapter in adapters:
            if adapter.address == wanted:
                return adapter
        raise ConfigError(
            f"no controller with address {wanted} "
            f"(have: {', '.join(f'{a.hci}={a.address}' for a in adapters) or 'none'})"
        )
    raise ConfigError(f"adapter spec {spec!r} is neither 'hciN' nor a controller address")


def resolve_roles(config: AdapterConfig, adapters: list[AdapterInfo]) -> ResolvedRoles:
    """Map the [adapter] config onto discovered controllers; pick the mode."""
    scan = resolve_spec(config.scan, adapters)
    transfer = [resolve_spec(spec, adapters) for spec in config.transfer]

    seen: set[str] = set()
    for adapter in transfer:
        if adapter.address in seen:
            raise ConfigError(
                f"adapter {adapter.hci} ({adapter.address}) listed twice in adapter.transfer"
            )
        seen.add(adapter.address)

    single = len(transfer) == 1 and transfer[0].address == scan.address
    if not single and scan.address in seen:
        raise ConfigError(
            f"scan adapter {scan.hci} ({scan.address}) also appears in adapter.transfer — "
            "a paused-scanner pool is not a supported mode; dedicate the scan adapter "
            "or configure exactly one shared adapter for single-adapter mode"
        )
    return ResolvedRoles(scan=scan, transfer=transfer, single_adapter_mode=single)
