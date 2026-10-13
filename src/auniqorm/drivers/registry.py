# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""aUniqORM driver registry for automatic driver selection based on URL protocol."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, cast

from auniqorm.exceptions import DriverNotFoundError

if TYPE_CHECKING:
    from auniqorm.drivers import BaseDriver

_DRIVER_CLASS_RE = re.compile(r"class (.*)\(BaseDriver\)", re.M)
_NATIVE_PROTOCOLS = {
    "postgresql": ("AUNIQORM_PG_NATIVE", "postgresql_native"),
    "mysql": ("AUNIQORM_MYSQL_NATIVE", "mysql_native"),
    "sqlite": ("AUNIQORM_SQLITE_NATIVE", "sqlite_native"),
}


def _driver_class_name(path: Path) -> str | None:
    """Return the BaseDriver subclass declared in *path*, if it declares one."""
    match = _DRIVER_CLASS_RE.search(path.read_text())
    return match.group(1) if match else None


def _discover_drivers() -> dict[str, tuple[str, str]]:
    """Map protocol -> (module path, driver class).

    Modules declaring no driver (helpers, base classes) are skipped rather than
    breaking the import.
    """
    found: dict[str, tuple[str, str]] = {}
    for path in Path(__file__).parent.rglob("*.py"):
        if path.stem in ("__init__", "registry"):
            continue
        class_name = _driver_class_name(path)
        if class_name is None:
            continue
        protocol = path.stem.lower()
        found[protocol] = (f"auniqorm.drivers.{protocol}", class_name)
    return found


class DriverRegistry:
    """Registry for database drivers.

    Maps URL protocols to their corresponding driver classes.
    Drivers are loaded lazily to avoid importing unavailable packages.
    """

    # Protocol -> (module_path, class_name)
    _drivers: dict[str, tuple[str, str]] = _discover_drivers()
    if "mongo" in _drivers:
        _drivers["mongodb"] = _drivers["mongo"]

    @classmethod
    def get(cls, protocol: str) -> type[BaseDriver]:
        """Get the driver class for a given protocol.

        Args:
            protocol: Database protocol (e.g., 'postgresql', 'mysql', 'sqlite')

        Returns:
            Driver class

        Raises:
            DriverNotFoundError: If no driver is registered for the protocol
        """
        try:
            import importlib

            protocol = {"bolt": "neo4j"}.get(protocol, protocol)
            native_setting = _NATIVE_PROTOCOLS.get(protocol)
            if native_setting is not None:
                env_name, native_protocol = native_setting
                if os.getenv(env_name, "1").lower() not in {"0", "false", "no", "off"}:
                    module_path, class_name = cls._drivers[native_protocol]
                    module = importlib.import_module(module_path)
                    return cast("type[BaseDriver]", getattr(module, class_name))

            module_path, class_name = cls._drivers[protocol]

            module = importlib.import_module(module_path)
            return cast("type[BaseDriver]", getattr(module, class_name))
        except ImportError as e:
            raise DriverNotFoundError(protocol) from e
        except KeyError as e:
            available = ", ".join(sorted(cls._drivers))
            raise DriverNotFoundError(
                protocol, f"{protocol} not registered, available protocols: {available}"
            ) from e

    @classmethod
    def register(
        cls,
        protocol: str,
        module_path: str,
        class_name: str,
    ) -> None:
        """Register a new driver.

        Args:
            protocol: Database protocol
            module_path: Full module path to the driver
            class_name: Driver class name
        """
        cls._drivers[protocol] = (module_path, class_name)

    @classmethod
    def available_protocols(cls) -> list[str]:
        """Get list of available protocols.

        Returns:
            List of currently registered protocol names
        """
        return list(cls._drivers)
