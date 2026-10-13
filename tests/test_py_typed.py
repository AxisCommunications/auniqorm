# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""PEP 561 marker must ship so downstream type checkers read auniqorm's types."""

from importlib.resources import files


def test_py_typed_is_packaged() -> None:
    marker = files("auniqorm").joinpath("py.typed")
    assert marker.is_file(), (
        "py.typed marker missing from the installed auniqorm package"
    )
