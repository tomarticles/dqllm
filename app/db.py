from __future__ import annotations

from typing import Any, Mapping, Sequence

import psycopg
from psycopg.rows import dict_row

from app.settings import settings


def open_pg_connection() -> psycopg.Connection:
    return psycopg.connect(settings.postgres_dsn, row_factory=dict_row)


def run_select_query(
    query: str,
    params: Sequence[Any] | Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    statement = query.strip()
    lowered = statement.lower()

    if not lowered.startswith("select"):
        raise ValueError("Only SELECT queries are allowed")
    if ";" in statement:
        raise ValueError("Semicolons are not allowed in SELECT queries")

    with open_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(statement, params)
            rows = cur.fetchall()

    return [dict(row) for row in rows]
