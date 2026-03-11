from __future__ import annotations

from typing import Any

from app.db import run_select_query


def _normalize_severity(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"low", "medium", "high"}:
        return normalized
    return "medium"


def _normalize_sql_check(query: str) -> str:
    statement = (query or "").strip()
    while statement.endswith(";"):
        statement = statement[:-1].rstrip()
    return statement


def _is_safe_select_sql(query: str) -> bool:
    statement = _normalize_sql_check(query)
    if not statement:
        return False
    if not statement.lower().startswith("select"):
        return False
    if ";" in statement:
        return False
    return True


def execute_rule_sql_check(rule: dict[str, Any], sample_limit: int = 5) -> dict[str, Any]:
    rule_name = str(rule.get("rule_name", "")).strip()
    description = str(rule.get("description", "")).strip()
    severity = _normalize_severity(rule.get("severity"))
    sql_check = _normalize_sql_check(str(rule.get("sql_check", "")))

    base_result = {
        "rule_name": rule_name,
        "description": description,
        "severity": severity,
        "sql_check": sql_check,
        "success": False,
        "status": "failure",
        "result_summary": "",
        "sample_failing_rows": [],
    }

    if not rule_name:
        base_result["result_summary"] = "Missing rule_name"
        return base_result

    if not _is_safe_select_sql(sql_check):
        base_result["result_summary"] = "Rejected SQL check: only single-statement SELECT queries are allowed"
        return base_result

    try:
        rows = run_select_query(sql_check)
        failing_row_count = len(rows)
        sample_rows = rows[: max(sample_limit, 0)]

        if failing_row_count == 0:
            summary = "No failing rows returned by SQL check"
        else:
            summary = f"{failing_row_count} failing row(s) returned by SQL check"

        base_result["success"] = True
        base_result["status"] = "success"
        base_result["result_summary"] = summary
        base_result["sample_failing_rows"] = sample_rows
        base_result["failing_row_count"] = failing_row_count
        return base_result
    except Exception as exc:
        base_result["result_summary"] = f"SQL execution error: {type(exc).__name__}: {exc}"
        return base_result


def execute_data_quality_checks(
    generated_rules: dict[str, Any],
    sample_limit: int = 5,
) -> dict[str, Any]:
    table_name = str(generated_rules.get("table_name", "")).strip()
    raw_rules = generated_rules.get("rules", [])

    rules: list[dict[str, Any]] = raw_rules if isinstance(raw_rules, list) else []
    results = [execute_rule_sql_check(rule, sample_limit=sample_limit) for rule in rules]

    success_count = sum(1 for item in results if item.get("success") is True)
    failure_count = len(results) - success_count
    execution_error_count = failure_count
    rule_violation_count = sum(
        1
        for item in results
        if item.get("success") is True and int(item.get("failing_row_count") or 0) > 0
    )

    return {
        "table_name": table_name,
        "total_rules": len(results),
        "success_count": success_count,
        "failure_count": failure_count,
        "execution_error_count": execution_error_count,
        "rule_violation_count": rule_violation_count,
        "results": results,
    }
