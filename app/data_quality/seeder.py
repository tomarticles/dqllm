from __future__ import annotations

from pathlib import Path

from app.db import open_pg_connection


DEFAULT_SEED_FILE = Path(__file__).resolve().parents[2] / "data" / "postgres" / "init" / "01_dq_demo.sql"


def _split_sql_statements(script: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False

    for ch in script:
        if ch == "'":
            in_single_quote = not in_single_quote
            current.append(ch)
            continue

        if ch == ";" and not in_single_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            continue

        current.append(ch)

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)

    return statements


def seed_demo_dataset(seed_file: Path | None = None) -> dict[str, str | int]:
    file_path = seed_file or DEFAULT_SEED_FILE
    if not file_path.exists():
        raise FileNotFoundError(f"Seed SQL file not found: {file_path}")

    script = file_path.read_text(encoding="utf-8")
    if not script.strip():
        raise ValueError(f"Seed SQL file is empty: {file_path}")

    statements = _split_sql_statements(script)
    if not statements:
        raise ValueError(f"No SQL statements found in seed file: {file_path}")

    with open_pg_connection() as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)

    return {
        "seed_file": str(file_path),
        "statements_executed": len(statements),
    }
