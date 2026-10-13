# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""aUniqORM exceptions."""


class AUniqORMError(Exception):
    """Base exception for all aUniqORM errors."""


class ConnectionError(AUniqORMError):
    """Raised when a database connection fails."""


class DriverNotFoundError(AUniqORMError):
    """Raised when the required database driver is not installed."""

    def __init__(self, protocol: str, msg: str | None = None) -> None:
        self.protocol = protocol
        super().__init__(
            f"Driver for '{protocol}' not found. "
            f"Install it with: pip install auniqorm[{protocol}]"
            f"{msg or ''}"
        )


class QueryError(AUniqORMError):
    """Raised when a query execution fails."""


class SchemaMigrationError(AUniqORMError):
    """Raised when automatic schema migration fails.

    aUniqORM can only add new columns. Changes that require altering or
    dropping existing columns are beyond automatic migration and need
    manual intervention or a dedicated migration tool.
    """

    def __init__(self, table: str, columns: list[str], cause: Exception) -> None:
        self.table = table
        self.columns = columns
        self.cause = cause
        super().__init__(
            f"Migration of table '{table}' failed for columns {columns}. "
            f"This exceeds aUniqORM's automatic migration capability "
            f"(add-column only). Consider making new columns nullable, "
            f"adding a server_default, or using a migration tool. "
            f"Cause: {cause}"
        )
