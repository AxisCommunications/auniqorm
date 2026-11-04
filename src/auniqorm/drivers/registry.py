# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""aUniqORM driver registry for automatic driver selection based on URL protocol."""

from __future__ import annotations

import importlib
import importlib.util
import logging
import os
import re
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from auniqorm.exceptions import DriverNotFoundError

if TYPE_CHECKING:
    from auniqorm.drivers import BaseDriver

logger = logging.getLogger("auniqorm")

_DRIVER_CLASS_RE = re.compile(r"class (.*)\(BaseDriver\)", re.M)
_NATIVE_PROTOCOLS = {
    "postgresql": ("AUNIQORM_PG_NATIVE", "postgresql_native"),
    "mysql": ("AUNIQORM_MYSQL_NATIVE", "mysql_native"),
    "sqlite": ("AUNIQORM_SQLITE_NATIVE", "sqlite_native"),
}
# Third-party package each pure-Python fallback driver needs.
_FALLBACK_PACKAGES = {
    "postgresql": ("asyncpg", "postgres"),
    "mysql": ("aiomysql", "mysql"),
    "sqlite": ("aiosqlite", "sqlite"),
}


def native_options(
    options: Mapping[str, Any], ssl: Callable[[Any], dict[str, Any] | None]
) -> tuple[dict[str, Any], list[str]]:
    """Split ``Database`` options into native ``connect_*`` arguments and the
    names of options a native driver cannot honour: anything but ``ssl``
    values *ssl* translates and non-negative ``min_size``/``max_size``."""
    kwargs: dict[str, Any] = {}
    unsupported: list[str] = []
    for name, value in options.items():
        if name == "ssl":
            extra = {} if value is None else ssl(value)
        elif name in ("min_size", "max_size") and type(value) is int and value >= 0:
            extra = {name: value}
        else:
            extra = None
        if extra is None:
            unsupported.append(name)
        else:
            kwargs.update(extra)
    return kwargs, unsupported


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
    def get(
        cls, protocol: str, options: Mapping[str, Any] | None = None
    ) -> type[BaseDriver]:
        """Get the driver class for a given protocol.

        The native (Rust) driver is preferred unless disabled through its
        environment variable. If *options* contains keyword options the native
        driver cannot honour faithfully (for example an ``ssl.SSLContext``),
        the pure-Python driver is selected instead, so options are never
        silently ignored.

        Args:
            protocol: Database protocol (e.g., 'postgresql', 'mysql', 'sqlite')
            options: The ``Database`` keyword options the driver will receive

        Returns:
            Driver class

        Raises:
            DriverNotFoundError: If no driver is registered for the protocol,
                or the options need the pure-Python driver and its package is
                not installed
        """
        try:
            native_setting = _NATIVE_PROTOCOLS.get(protocol)
            if native_setting is not None:
                env_name, native_protocol = native_setting
                if os.getenv(env_name, "1").lower() not in {"0", "false", "no", "off"}:
                    module_path, class_name = cls._drivers[native_protocol]
                    module = importlib.import_module(module_path)
                    native = cast("type[BaseDriver]", getattr(module, class_name))
                    check = getattr(native, "unsupported_options", None)
                    unsupported = check(options or {}) if check else []
                    if not unsupported:
                        return native
                    cls._require_fallback(protocol, env_name, unsupported)

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

    @staticmethod
    def _require_fallback(protocol: str, env_name: str, unsupported: list[str]) -> None:
        """Ensure the pure-Python driver can take options the native one can't."""
        package, extra = _FALLBACK_PACKAGES.get(protocol, (None, protocol))
        names = ", ".join(sorted(unsupported))
        if package is not None and importlib.util.find_spec(package) is None:
            raise DriverNotFoundError(
                protocol,
                f": the native driver cannot honour option(s) {names}, and the "
                f"fallback driver needs {package} (pip install auniqorm[{extra}])",
            )
        logger.info(
            "Using the pure-Python %s driver: the native driver cannot honour "
            "option(s) %s (set %s=0 to always use it)",
            protocol,
            names,
            env_name,
        )

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
