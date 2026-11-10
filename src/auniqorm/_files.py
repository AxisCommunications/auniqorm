# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""File helpers shared by the SQLite drivers."""

from __future__ import annotations

import os


def create_private_file(path: str) -> None:
    """Create *path* readable by its owner only, unless it already exists.

    SQLite gives the ``-wal`` and ``-shm`` files the mode of the database file,
    so creating it first keeps all three out of reach of other local users.
    """
    if not path or path == ":memory:" or path.startswith("file:"):
        return
    try:
        os.close(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))
    except OSError:
        # Already exists, or unopenable: SQLite reports the latter itself.
        pass
