from __future__ import annotations

from typing import Any

from psycopg import sql

from app.db import open_pg_connection, run_select_query


def list_tables(schema_name: str = "public") -> list[str]:
    rows = run_select_query(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (schema_name,),
    )
    return [str(row["table_name"]) for row in rows if row.get("table_name")]


def table_exists(table_name: str, schema_name: str = "public") -> bool:
    rows = run_select_query(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        ) AS exists
        """,
        (schema_name, table_name),
    )
    return bool(rows and rows[0].get("exists"))


def get_table_schema(table_name: str, schema_name: str = "public") -> dict[str, Any]:
    rows = run_select_query(
        """
        SELECT
            column_name,
            data_type,
            is_nullable,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema_name, table_name),
    )

    columns = [
        {
            "name": row["column_name"],
            "data_type": row["data_type"],
            "nullable": row["is_nullable"] == "YES",
        }
        for row in rows
    ]

    return {
        "schema": schema_name,
        "table": table_name,
        "columns": columns,
    }


def get_table_sample_rows(
    table_name: str,
    schema_name: str = "public",
    sample_size: int = 5,
) -> list[dict[str, Any]]:
    if sample_size <= 0:
        return []
    if not table_exists(table_name, schema_name=schema_name):
        raise ValueError(f"Table not found: {schema_name}.{table_name}")

    with open_pg_connection() as conn:
        with conn.cursor() as cur:
            query = sql.SQL("SELECT * FROM {}.{} LIMIT {}").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
                sql.Literal(sample_size),
            )
            cur.execute(query)
            rows = cur.fetchall()

    return [dict(row) for row in rows]


def get_table_profile(table_name: str, schema_name: str = "public") -> dict[str, Any]:
    if not table_exists(table_name, schema_name=schema_name):
        raise ValueError(f"Table not found: {schema_name}.{table_name}")

    schema = get_table_schema(table_name, schema_name=schema_name)
    columns = schema["columns"]

    with open_pg_connection() as conn:
        with conn.cursor() as cur:
            row_count_query = sql.SQL("SELECT COUNT(*) AS row_count FROM {}.{}").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
            cur.execute(row_count_query)
            row_count_result = cur.fetchone() or {"row_count": 0}
            row_count = int(row_count_result["row_count"])

            column_stats: list[dict[str, Any]] = []
            for column in columns:
                col_name = column["name"]
                stats_query = sql.SQL(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE {} IS NULL) AS null_count,
                        COUNT(DISTINCT {}) AS distinct_count
                    FROM {}.{}
                    """
                ).format(
                    sql.Identifier(col_name),
                    sql.Identifier(col_name),
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                )
                cur.execute(stats_query)
                stats = cur.fetchone() or {"null_count": 0, "distinct_count": 0}

                column_stats.append(
                    {
                        "column": col_name,
                        "null_count": int(stats["null_count"]),
                        "distinct_count": int(stats["distinct_count"]),
                    }
                )

    return {
        "schema": schema_name,
        "table": table_name,
        "row_count": row_count,
        "column_stats": column_stats,
    }
