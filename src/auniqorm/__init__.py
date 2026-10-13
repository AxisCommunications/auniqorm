# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""aUniqORM - A lightweight Python ORM library with multi-database support."""

from auniqorm.columns import Column, ForeignKey, PrimaryKey
from auniqorm.connection import Database, dynamic_model
from auniqorm.exceptions import (
    AUniqORMError,
    ConnectionError,
    DriverNotFoundError,
    QueryError,
    SchemaMigrationError,
)
from auniqorm.models import (
    Aggregate,
    AUniqORMBase,
    Avg,
    Count,
    F,
    ManyToMany,
    Max,
    Min,
    ModelRegistry,
    Q,
    RawSQL,
    Sum,
)
from auniqorm.schema import SchemaDiff, SchemaDiffResult

try:
    from auniqorm._version import __version__
except ModuleNotFoundError:  # editable install without build
    __version__ = "0.0.0-dev"
__all__ = [
    "Database",
    "dynamic_model",
    "AUniqORMBase",
    "ModelRegistry",
    "Q",
    "RawSQL",
    "F",
    # Aggregates
    "Aggregate",
    "Count",
    "Sum",
    "Avg",
    "Min",
    "Max",
    # Relationships
    "ManyToMany",
    # Columns
    "Column",
    "PrimaryKey",
    "ForeignKey",
    # Schema
    "SchemaDiff",
    "SchemaDiffResult",
    # Exceptions
    "AUniqORMError",
    "ConnectionError",
    "DriverNotFoundError",
    "QueryError",
    "SchemaMigrationError",
]
