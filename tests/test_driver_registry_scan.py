# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""The driver registry scans drivers/*.py, so it must tolerate non-driver files."""

from __future__ import annotations

from pathlib import Path

from auniqorm.drivers.registry import DriverRegistry, _driver_class_name


def test_finds_the_driver_class_name(tmp_path: Path) -> None:
    module = tmp_path / "widget.py"
    module.write_text("class WidgetDriver(BaseDriver):\n    pass\n")

    assert _driver_class_name(module) == "WidgetDriver"


def test_skips_a_module_with_no_driver(tmp_path: Path) -> None:
    """A helper module alongside the drivers must not crash the import."""
    module = tmp_path / "helpers.py"
    module.write_text("def shared() -> None:\n    pass\n")

    assert _driver_class_name(module) is None


def test_registers_the_shipped_drivers() -> None:
    assert set(DriverRegistry._drivers) >= {"postgresql", "mysql", "sqlite"}
    for module_path, class_name in DriverRegistry._drivers.values():
        assert module_path.startswith("auniqorm.drivers.")
        assert class_name
