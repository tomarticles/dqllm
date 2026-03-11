from __future__ import annotations

import json
import re
from typing import Any

from app.llm import generate_answer


def build_rule_generation_prompt(
    table_name: str,
    schema: dict[str, Any],
    sample_rows: list[dict[str, Any]],
    profile: dict[str, Any] | None = None,
    max_rules: int = 10,
) -> str:
    profile_section = (
        json.dumps(profile, ensure_ascii=False, indent=2, default=str)
        if profile
        else "null"
    )
    sample_section = json.dumps(sample_rows, ensure_ascii=False, indent=2, default=str)
    schema_section = json.dumps(schema, ensure_ascii=False, indent=2, default=str)

    return f"""
You are a senior data quality analyst for relational databases.

Task:
- Propose practical data quality rules for table "{table_name}".
- Focus on relational data quality checks suitable for SQL.
- Prefer rules that can be executed directly as SQL checks.
- Keep output concise and useful for automation.

Input context:
table_name: {table_name}
schema:
{schema_section}

sample_rows:
{sample_section}

profile:
{profile_section}

Output requirements:
- Return JSON only (no markdown, no comments, no explanations outside JSON).
- Return a single JSON object with this exact shape:
{{
  "table_name": "{table_name}",
  "rules": [
    {{
      "rule_name": "short unique name",
      "description": "what this checks",
      "severity": "low|medium|high",
      "sql_check": "SELECT ...",
      "rationale": "why this matters"
    }}
  ]
}}
- Propose at most {max_rules} rules.
- Include only rules relevant to the provided context.
- If uncertain, return fewer rules rather than guessing.
""".strip()


def build_rule_repair_prompt(
    table_name: str,
    raw_response: str,
    max_rules: int = 10,
) -> str:
    return f"""
Convert the following model output into valid JSON only.

Target JSON shape:
{{
  "table_name": "{table_name}",
  "rules": [
    {{
      "rule_name": "short unique name",
      "description": "what this checks",
      "severity": "low|medium|high",
      "sql_check": "SELECT ...",
      "rationale": "why this matters"
    }}
  ]
}}

Rules:
- Return JSON object only. No markdown.
- Keep at most {max_rules} rules.
- If input has no usable rules, return "rules": [].
- SQL checks should be single SELECT statements.

Input to repair:
{raw_response[:3000]}
""".strip()


def _safe_json_object(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    if not raw:
        return {}

    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass

    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return {}

    try:
        parsed = json.loads(raw[start : end + 1])
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _normalize_severity(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"low", "medium", "high"}:
        return normalized
    return "medium"


def _normalize_sql_check(value: Any) -> str:
    query = str(value or "").strip()
    while query.endswith(";"):
        query = query[:-1].rstrip()
    if not query:
        return ""
    if not query.lower().startswith("select"):
        return ""
    if ";" in query:
        return ""
    return query


def _adjust_sql_check_from_rule_text(rule_name: str, description: str, sql_check: str) -> str:
    query = sql_check
    context = f"{rule_name} {description}".lower()
    lowered_query = query.lower()

    if (
        "email" in context
        and "invalid" in context
        and "email" in lowered_query
        and "!~" not in lowered_query
    ):
        query = re.sub(r"(\bemail\b\s*)~", r"\1!~", query, count=1, flags=re.IGNORECASE)

    return query


def _normalize_rule(item: Any) -> dict[str, str] | None:
    if not isinstance(item, dict):
        return None

    rule_name = str(
        item.get("rule_name")
        or item.get("name")
        or item.get("title")
        or ""
    ).strip()
    description = str(
        item.get("description")
        or item.get("rule_description")
        or item.get("check")
        or ""
    ).strip()
    sql_check = _normalize_sql_check(
        item.get("sql_check")
        or item.get("sql")
        or item.get("query")
        or item.get("check_sql")
        or ""
    )
    rationale = str(
        item.get("rationale")
        or item.get("reason")
        or item.get("why")
        or ""
    ).strip()
    severity = _normalize_severity(
        item.get("severity")
        or item.get("priority")
        or item.get("level")
    )
    sql_check = _adjust_sql_check_from_rule_text(rule_name, description, sql_check)

    if not rule_name or not description:
        return None

    return {
        "rule_name": rule_name,
        "description": description,
        "severity": severity,
        "sql_check": sql_check,
        "rationale": rationale,
    }


def parse_generated_rules(response_text: str, table_name: str) -> dict[str, Any]:
    parsed = _safe_json_object(response_text)
    raw_rules = (
        parsed.get("rules")
        or parsed.get("data_quality_rules")
        or parsed.get("proposed_rules")
        or []
    ) if isinstance(parsed, dict) else []

    rules: list[dict[str, str]] = []
    if isinstance(raw_rules, list):
        for item in raw_rules:
            normalized = _normalize_rule(item)
            if normalized:
                rules.append(normalized)

    if rules:
        return {
            "table_name": str(parsed.get("table_name") or table_name),
            "rules": rules,
        }

    return {
        "table_name": table_name,
        "rules": [],
        "parser_warning": "LLM response was not valid expected JSON rules output",
        "raw_response": (response_text or "").strip()[:1500],
    }


def generate_data_quality_rules(
    table_name: str,
    schema: dict[str, Any],
    sample_rows: list[dict[str, Any]],
    profile: dict[str, Any] | None = None,
    max_rules: int = 10,
) -> dict[str, Any]:
    prompt = build_rule_generation_prompt(
        table_name=table_name,
        schema=schema,
        sample_rows=sample_rows,
        profile=profile,
        max_rules=max_rules,
    )
    response = generate_answer(prompt)
    first_pass = parse_generated_rules(response, table_name=table_name)
    if first_pass.get("rules"):
        return first_pass

    repair_prompt = build_rule_repair_prompt(
        table_name=table_name,
        raw_response=response,
        max_rules=max_rules,
    )
    repaired_response = generate_answer(repair_prompt)
    repaired = parse_generated_rules(repaired_response, table_name=table_name)
    if repaired.get("rules"):
        repaired["repair_attempted"] = True
        return repaired

    first_pass["repair_attempted"] = True
    return first_pass
