"""Step 20: adapter spec parsing, role resolution, and mode selection."""

import pytest

from tempo_tb_ingest.adapters import AdapterInfo, resolve_roles, resolve_spec
from tempo_tb_ingest.config import AdapterConfig, ConfigError

BUILTIN = AdapterInfo(hci="hci0", address="44:A3:BB:E8:AD:D1", name="builtin", powered=True)
DONGLE_A = AdapterInfo(hci="hci1", address="DC:DF:ED:91:91:1D", name="d1", powered=True)
DONGLE_B = AdapterInfo(hci="hci2", address="ED:BB:AD:89:36:98", name="d2", powered=True)
DONGLE_C = AdapterInfo(hci="hci3", address="F5:26:06:3E:53:3A", name="d3", powered=True)
ALL = [BUILTIN, DONGLE_A, DONGLE_B, DONGLE_C]


class TestResolveSpec:
    def test_by_hci_name(self) -> None:
        assert resolve_spec("hci2", ALL) is DONGLE_B

    def test_by_address_case_insensitive(self) -> None:
        assert resolve_spec("dc:df:ed:91:91:1d", ALL) is DONGLE_A
        assert resolve_spec("DC:DF:ED:91:91:1D", ALL) is DONGLE_A

    def test_missing_hci(self) -> None:
        with pytest.raises(ConfigError, match=r"hci9.*not present"):
            resolve_spec("hci9", ALL)

    def test_missing_address_lists_inventory(self) -> None:
        with pytest.raises(ConfigError, match=r"no controller with address.*hci0=44:A3"):
            resolve_spec("00:11:22:33:44:55", ALL)

    def test_garbage_spec(self) -> None:
        with pytest.raises(ConfigError, match="neither"):
            resolve_spec("the-blue-one", ALL)


class TestResolveRoles:
    def test_pool_mode(self) -> None:
        config = AdapterConfig(
            scan="44:A3:BB:E8:AD:D1",
            transfer=["hci1", "ED:BB:AD:89:36:98", "hci3"],
        )
        roles = resolve_roles(config, ALL)
        assert roles.single_adapter_mode is False
        assert roles.scan is BUILTIN
        assert roles.transfer == [DONGLE_A, DONGLE_B, DONGLE_C]

    def test_single_adapter_mode_same_entry(self) -> None:
        config = AdapterConfig(scan="hci0", transfer=["44:a3:bb:e8:ad:d1"])
        roles = resolve_roles(config, ALL)
        assert roles.single_adapter_mode is True
        assert roles.transfer == [BUILTIN]

    def test_scan_inside_pool_rejected(self) -> None:
        config = AdapterConfig(scan="hci0", transfer=["hci0", "hci1"])
        with pytest.raises(ConfigError, match=r"also appears in adapter\.transfer"):
            resolve_roles(config, ALL)

    def test_duplicate_transfer_rejected_across_spec_styles(self) -> None:
        config = AdapterConfig(scan="hci0", transfer=["hci1", "dc:df:ed:91:91:1d"])
        with pytest.raises(ConfigError, match="listed twice"):
            resolve_roles(config, ALL)

    def test_unresolvable_transfer_entry(self) -> None:
        config = AdapterConfig(scan="hci0", transfer=["hci7"])
        with pytest.raises(ConfigError, match="hci7"):
            resolve_roles(config, ALL)

    def test_default_config_is_single_adapter_mode(self) -> None:
        roles = resolve_roles(AdapterConfig(), ALL)  # scan=hci0, transfer=[hci0]
        assert roles.single_adapter_mode is True
