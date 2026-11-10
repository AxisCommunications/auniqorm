# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Helpers shared by test modules."""

from __future__ import annotations

from auniqorm.connection import Database
from auniqorm.models import AUniqORMBase, ModelRegistry


async def ready(
    db: Database, *models: type[AUniqORMBase], register: bool = False
) -> None:
    """Bind models to db and create their tables.

    ``register=True`` re-registers module-level models the autouse fixture cleared.
    """
    for m in models:
        if register:
            ModelRegistry.register(m)
    ModelRegistry.bind(db)
    for m in models:
        await m.create_table()
