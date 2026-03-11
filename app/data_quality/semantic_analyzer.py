from __future__ import annotations

import json
from typing import Any

from app.llm import generate_answer


def build_semantic_analysis_prompt(
    table_name: str,
    schema: dict[str, Any],
    sample_rows: list[dict[str, Any]],
    profile: dict[str, Any] | None = None,
    max_findings: int = 5,
) -> str:
    schema_section = json.dumps(schema, ensure_ascii=False, indent=2, default=str)
    samples_section = json.dumps(sample_rows, ensure_ascii=False, indent=2, default=str)
    profile_section = (
        json.dumps(profile, ensure_ascii=False, indent=2, default=str)
        if profile
        else "null"
    )

    return f"""
You are a data quality analyst focused on semantic inconsistencies in relational data.

Task:
- Analyze table "{table_name}" using the schema and sample rows.
- Identify likely semantic inconsistencies that are suspicious from a business perspective.
- Prefer practical findings that can be validated by SQL checks.

Examples of semantic inconsistencies:
- Status does not match dates (e.g., shipped before order date)
- Amount and status mismatches (e.g., paid status with null/zero amount)
- Values that conflict with likely meaning of the field

Context:
table_name: {table_name}
schema:
{schema_section}

sample_rows:
{samples_section}

profile:
{profile_section}

Output rules:
- Return JSON only, no markdown.
- Return one JSON object with exact shape:
{{
  "table_name": "{table_name}",
  "semantic_inconsistencies": [
    {{
      "issue_name": "short_issue_name",
      "description": "what looks semantically inconsistent",
      "severity": "low|medium|high",
      "confidence": "low|medium|high",
      "evidence_rows": [{{}}],
      "rationale": "why this is suspicious",
      "suggested_sql_check": "SELECT ..."
    }}
  ]
}}
- Return at most {max_findings} findings.
- If no clear inconsistency is present, return an empty list.
""".strip()


def build_semantic_repair_prompt(
    table_name: str,
    raw_response: str,
    max_findings: int = 5,
) -> str:
    return f"""
Convert the following model output into valid JSON only.

Target JSON shape:
{{
  "table_name": "{table_name}",
  "semantic_inconsistencies": [
    {{
      "issue_name": "short_issue_name",
      "description": "what looks semantically inconsistent",
      "severity": "low|medium|high",
      "confidence": "low|medium|high",
      "evidence_rows": [{{}}],
      "rationale": "why this is suspicious",
      "suggested_sql_check": "SELECT ..."
    }}
  ]
}}

Rules:
- Return JSON object only, no markdown.
- Keep at most {max_findings} findings.
- If input has no usable findings, return an empty list.

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


def _normalize_level(value: Any, default: str = "medium") -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"low", "medium", "high"}:
        return normalized
    return default


def _normalize_sql_suggestion(value: Any) -> str:
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


def _normalize_finding(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None

    issue_name = str(item.get("issue_name", "")).strip()
    description = str(item.get("description", "")).strip()
    rationale = str(item.get("rationale", "")).strip()
    suggested_sql_check = _normalize_sql_suggestion(item.get("suggested_sql_check", ""))
    severity = _normalize_level(item.get("severity"), default="medium")
    confidence = _normalize_level(item.get("confidence"), default="medium")

    evidence_rows = item.get("evidence_rows", [])
    if not isinstance(evidence_rows, list):
        evidence_rows = []

    if not issue_name or not description:
        return None

    cleaned_evidence: list[dict[str, Any]] = []
    for row in evidence_rows[:5]:
        if isinstance(row, dict):
            cleaned_evidence.append(row)

    return {
        "issue_name": issue_name,
        "description": description,
        "severity": severity,
        "confidence": confidence,
        "evidence_rows": cleaned_evidence,
        "rationale": rationale,
        "suggested_sql_check": suggested_sql_check,
    }


def parse_semantic_inconsistencies(response_text: str, table_name: str) -> dict[str, Any]:
    parsed = _safe_json_object(response_text)
    raw_items = parsed.get("semantic_inconsistencies", []) if isinstance(parsed, dict) else []

    findings: list[dict[str, Any]] = []
    if isinstance(raw_items, list):
        for item in raw_items:
            normalized = _normalize_finding(item)
            if normalized:
                findings.append(normalized)

    result = {
        "table_name": str(parsed.get("table_name") or table_name),
        "semantic_inconsistencies": findings,
    }

    if not findings and (response_text or "").strip():
        result["semantic_parser_warning"] = "LLM response was not valid expected semantic inconsistency JSON output"
        result["semantic_raw_response"] = (response_text or "").strip()[:1500]

    return result


def generate_semantic_inconsistencies(
    table_name: str,
    schema: dict[str, Any],
    sample_rows: list[dict[str, Any]],
    profile: dict[str, Any] | None = None,
    max_findings: int = 5,
) -> dict[str, Any]:
    prompt = build_semantic_analysis_prompt(
        table_name=table_name,
        schema=schema,
        sample_rows=sample_rows,
        profile=profile,
        max_findings=max_findings,
    )
    response = generate_answer(prompt)
    first_pass = parse_semantic_inconsistencies(response, table_name=table_name)
    if first_pass.get("semantic_inconsistencies"):
        return first_pass

    repair_prompt = build_semantic_repair_prompt(
        table_name=table_name,
        raw_response=response,
        max_findings=max_findings,
    )
    repaired_response = generate_answer(repair_prompt)
    repaired = parse_semantic_inconsistencies(repaired_response, table_name=table_name)
    if repaired.get("semantic_inconsistencies"):
        repaired["semantic_repair_attempted"] = True
        return repaired

    first_pass["semantic_repair_attempted"] = True
    return first_pass
