# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Opt-in raw-SQL placeholder normalization.

Callers may write portable raw SQL using a single canonical placeholder style
and have it translated to the connected dialect's native style just before
dispatch:

* ``qmark`` — ``?`` positional placeholders, bound from a sequence.
* ``named`` — ``:name`` placeholders, bound from a mapping.
* ``auto``  — ``named`` when params is a mapping, otherwise ``qmark``.

Native targets: ``$N`` for PostgreSQL, ``%s`` for MySQL (with literal ``%``
escaped to ``%%``), ``?`` for SQLite.

This is strictly opt-in (per call via ``paramstyle=``); the ORM's own SQL is
already native and never routed through here.  The expensive literal-aware
scan and value-extraction plan are cached per ``(query, style, target)`` so
repeated executions cost only a dict lookup plus value gathering.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from functools import lru_cache
from typing import Any, cast

from auniqorm.exceptions import QueryError

_VALID_STYLES = ("qmark", "named", "auto")

_TARGET_BY_DIALECT = {
    "postgresql": "numeric",  # $N
    "mysql": "format",  # %s, literal % escaped
    "sqlite": "qmark",  # ?
}


def _native_target(dialect: str) -> str:
    return _TARGET_BY_DIALECT.get(dialect, "qmark")


@lru_cache(maxsize=512)
def _build_plan(
    query: str, input_style: str, target: str
) -> tuple[str, tuple[str, ...] | None, int]:
    """Scan ``query`` once and return ``(translated_sql, names, count)``.

    ``names`` is the ordered tuple of bind names for ``named`` style (used to
    project a mapping into a positional list), or ``None`` for ``qmark``.
    ``count`` is the number of placeholders emitted.
    """
    out: list[str] = []
    names: list[str] = []
    count = 0
    idx = 1
    i = 0
    n = len(query)
    esc_percent = target == "format"

    def emit_placeholder() -> None:
        nonlocal idx, count
        count += 1
        if target == "numeric":
            out.append(f"${idx}")
            idx += 1
        elif target == "format":
            out.append("%s")
        else:
            out.append("?")

    while i < n:
        c = query[i]

        # Single-quoted string literal (with '' escape).
        if c == "'":
            out.append(c)
            i += 1
            while i < n:
                ch = query[i]
                if ch == "%" and esc_percent:
                    out.append("%%")
                    i += 1
                    continue
                out.append(ch)
                if ch == "'":
                    if i + 1 < n and query[i + 1] == "'":
                        out.append("'")
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue

        # Double-quoted identifier (with "" escape).
        if c == '"':
            out.append(c)
            i += 1
            while i < n:
                ch = query[i]
                out.append(ch)
                if ch == '"':
                    if i + 1 < n and query[i + 1] == '"':
                        out.append('"')
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue

        # Backtick identifier (MySQL).
        if c == "`":
            out.append(c)
            i += 1
            while i < n:
                ch = query[i]
                out.append(ch)
                i += 1
                if ch == "`":
                    break
            continue

        # Line comment.
        if c == "-" and i + 1 < n and query[i + 1] == "-":
            while i < n and query[i] != "\n":
                ch = query[i]
                out.append("%%" if (ch == "%" and esc_percent) else ch)
                i += 1
            continue

        # Block comment.
        if c == "/" and i + 1 < n and query[i + 1] == "*":
            out.append("/*")
            i += 2
            while i < n:
                if query[i] == "*" and i + 1 < n and query[i + 1] == "/":
                    out.append("*/")
                    i += 2
                    break
                ch = query[i]
                out.append("%%" if (ch == "%" and esc_percent) else ch)
                i += 1
            continue

        # Dollar-quoted string (PostgreSQL): $tag$ ... $tag$.
        if c == "$":
            j = i + 1
            while j < n and (query[j].isalnum() or query[j] == "_"):
                j += 1
            if j < n and query[j] == "$":
                tag = query[i : j + 1]
                out.append(tag)
                i = j + 1
                end = query.find(tag, i)
                body = query[i:] if end == -1 else query[i:end]
                out.append(body.replace("%", "%%") if esc_percent else body)
                if end == -1:
                    i = n
                else:
                    out.append(tag)
                    i = end + len(tag)
                continue
            # Not a dollar quote — fall through as a literal '$'.

        # qmark placeholder.
        if input_style == "qmark" and c == "?":
            emit_placeholder()
            i += 1
            continue

        # named placeholder (skip :: casts).
        if input_style == "named" and c == ":":
            if i + 1 < n and query[i + 1] == ":":
                out.append("::")
                i += 2
                continue
            j = i + 1
            while j < n and (query[j].isalnum() or query[j] == "_"):
                j += 1
            if j > i + 1:
                names.append(query[i + 1 : j])
                emit_placeholder()
                i = j
                continue

        if c == "%" and esc_percent:
            out.append("%%")
            i += 1
            continue

        out.append(c)
        i += 1

    return "".join(out), (tuple(names) if input_style == "named" else None), count


def _resolve(
    query: str, params: Any, dialect: str, style: str
) -> tuple[str, tuple[str, ...] | None, int, str]:
    if style not in _VALID_STYLES:
        raise QueryError(
            f"invalid paramstyle {style!r}; expected one of {_VALID_STYLES}"
        )
    target = _native_target(dialect)
    is_mapping = isinstance(params, Mapping)
    resolved = ("named" if is_mapping else "qmark") if style == "auto" else style
    sql, names, count = _build_plan(query, resolved, target)
    return sql, names, count, resolved


def normalize_query(
    query: str,
    params: Sequence[Any] | Mapping[str, Any] | None,
    dialect: str,
    style: str,
) -> tuple[str, list[Any] | None]:
    """Translate ``query`` to ``dialect``'s native placeholders.

    Returns ``(translated_sql, ordered_values)``.
    """
    sql, names, count, resolved = _resolve(query, params, dialect, style)

    if resolved == "named":
        if not isinstance(params, Mapping):
            raise QueryError("named paramstyle requires a mapping of parameters")
        try:
            values = [params[name] for name in (names or ())]
        except KeyError as exc:
            raise QueryError(f"missing bind parameter: {exc}") from exc
        return sql, values

    seq = list(params) if params is not None else []
    if len(seq) != count:
        raise QueryError(
            f"parameter count mismatch: query has {count} placeholders, "
            f"got {len(seq)} values"
        )
    return sql, (seq or None)


def normalize_many(
    query: str,
    rows: Sequence[Sequence[Any] | Mapping[str, Any]],
    dialect: str,
    style: str,
) -> tuple[str, list[list[Any]]]:
    """Translate a batch query and project each row into positional values."""
    if not rows:
        return query, []

    sql, names, _count, resolved = _resolve(query, rows[0], dialect, style)

    if resolved == "named":
        try:
            projected = [
                [cast("Mapping[str, Any]", row)[name] for name in (names or ())]
                for row in rows
            ]
        except KeyError as exc:
            raise QueryError(f"missing bind parameter: {exc}") from exc
        return sql, projected

    return sql, [list(row) for row in rows]
