# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Fast, cached detection of the benchmarks/compose.yaml services."""

from __future__ import annotations

import asyncio
import functools
import socket

PG_URL = "postgresql://bench:bench@localhost:5432/bench"
MYSQL_URL = "mysql://bench:bench@localhost:3306/bench"
MONGO_URL = "mongodb://localhost:27017/auniqorm_bridge_it"

# marker name -> (port, native connect function, connect args)
SERVICES = {
    "needs_pg": (5432, "connect_pg", (PG_URL,)),
    "needs_mysql": (3306, "connect_mysql", (MYSQL_URL,)),
    "needs_mongo": (27017, "connect_mongo", (MONGO_URL, "auniqorm_bridge_it")),
}


@functools.cache
def reachable(port: int) -> bool:
    try:
        socket.create_connection(("localhost", port), timeout=0.5).close()
    except OSError:
        return False
    return True


@functools.cache
def available(marker: str) -> bool:
    """True if the service accepts TCP and the native driver can connect."""
    port, connect_name, args = SERVICES[marker]
    if not reachable(port):
        return False
    import auniqorm_native

    async def check() -> bool:
        try:
            engine = await getattr(auniqorm_native, connect_name)(*args)
            await engine.close()
        except Exception:
            return False
        return True

    return asyncio.run(check())
