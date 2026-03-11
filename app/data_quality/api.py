from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.data_quality.check_executor import execute_data_quality_checks
from app.data_quality.db_introspection import (
    get_table_profile,
    get_table_sample_rows,
    get_table_schema,
    list_tables,
    table_exists,
)
from app.data_quality.rule_generator import generate_data_quality_rules
from app.data_quality.semantic_analyzer import generate_semantic_inconsistencies
from app.data_quality.seeder import seed_demo_dataset


dq_router = APIRouter(prefix="/dq", tags=["data-quality"])


class DQTablesResponse(BaseModel):
    status: str
    schema_name: str
    tables: list[str]


class DQAnalyzeRequest(BaseModel):
    table_name: str = Field(min_length=1)
    schema_name: str = "public"
    sample_size: int = Field(default=8, ge=1, le=100)
    include_profile: bool = True
    max_rules: int = Field(default=10, ge=1, le=25)
    sample_failing_rows: int = Field(default=5, ge=0, le=50)
    include_semantic_analysis: bool = True
    semantic_max_findings: int = Field(default=5, ge=1, le=15)


class DQColumnSchema(BaseModel):
    name: str
    data_type: str
    nullable: bool


class DQSchemaResponse(BaseModel):
    schema_name: str
    table: str
    columns: list[DQColumnSchema]


class DQColumnProfileStat(BaseModel):
    column: str
    null_count: int
    distinct_count: int


class DQProfileResponse(BaseModel):
    schema_name: str
    table: str
    row_count: int
    column_stats: list[DQColumnProfileStat]


class DQRule(BaseModel):
    rule_name: str
    description: str
    severity: str
    sql_check: str
    rationale: str


class DQRuleExecutionResult(BaseModel):
    rule_name: str
    description: str
    severity: str
    sql_check: str
    success: bool
    status: str
    result_summary: str
    sample_failing_rows: list[dict[str, Any]]
    failing_row_count: int | None = None


class DQCheckExecutionSummary(BaseModel):
    table_name: str
    total_rules: int
    success_count: int
    failure_count: int
    execution_error_count: int = 0
    rule_violation_count: int = 0
    results: list[DQRuleExecutionResult]


class DQIssue(BaseModel):
    rule_name: str
    description: str
    severity: str
    issue_type: str
    result_summary: str
    failed_row_count: int | None = None
    sample_failing_rows: list[dict[str, Any]] = Field(default_factory=list)


class DQIssuesBySeverity(BaseModel):
    high: list[DQIssue] = Field(default_factory=list)
    medium: list[DQIssue] = Field(default_factory=list)
    low: list[DQIssue] = Field(default_factory=list)


class DQSummary(BaseModel):
    table_analyzed: str
    rules_generated: int
    checks_executed: int
    passed_checks: int
    failed_checks: int
    execution_errors: int
    key_anomalies_found: int


class DQSemanticInconsistency(BaseModel):
    issue_name: str
    description: str
    severity: str
    confidence: str
    evidence_rows: list[dict[str, Any]] = Field(default_factory=list)
    rationale: str
    suggested_sql_check: str


class DQAnalyzeResponse(BaseModel):
    status: str
    summary: DQSummary
    data_quality_score: int | None
    issues_by_severity: DQIssuesBySeverity
    key_anomalies: list[str]
    suggested_next_steps: list[str]
    table_name: str
    schema_name: str
    schema_info: DQSchemaResponse
    sample_rows: list[dict[str, Any]]
    profile: DQProfileResponse | None = None
    semantic_inconsistencies: list[DQSemanticInconsistency] = Field(default_factory=list)
    generated_rules: list[DQRule]
    checks: DQCheckExecutionSummary
    parser_warning: str | None = None
    semantic_parser_warning: str | None = None


class DQSeedResponse(BaseModel):
    status: str
    seed_file: str
    statements_executed: int
    message: str


def _severity_bucket(severity: str) -> str:
    normalized = (severity or "").strip().lower()
    if normalized in {"low", "medium", "high"}:
        return normalized
    return "medium"


def _build_analysis_sections(
    table_name: str,
    generated_rules: list[dict[str, Any]],
    checks: dict[str, Any],
    semantic_inconsistencies: list[dict[str, Any]] | None = None,
) -> tuple[DQSummary, int | None, DQIssuesBySeverity, list[str], list[str]]:
    results = checks.get("results", [])
    if not isinstance(results, list):
        results = []

    passed_checks = 0
    failed_checks = 0
    execution_errors = 0

    grouped: dict[str, list[DQIssue]] = {"high": [], "medium": [], "low": []}
    key_anomalies: list[str] = []

    for item in results:
        if not isinstance(item, dict):
            continue

        severity = _severity_bucket(str(item.get("severity", "")))
        rule_name = str(item.get("rule_name", "")).strip() or "unnamed_rule"
        description = str(item.get("description", "")).strip()
        result_summary = str(item.get("result_summary", "")).strip()
        success = bool(item.get("success", False))
        failing_row_count_raw = item.get("failing_row_count")
        failing_row_count = int(failing_row_count_raw) if isinstance(failing_row_count_raw, int) else 0
        sample_rows = item.get("sample_failing_rows", [])
        if not isinstance(sample_rows, list):
            sample_rows = []

        if not success:
            execution_errors += 1
            issue = DQIssue(
                rule_name=rule_name,
                description=description,
                severity=severity,
                issue_type="execution_error",
                result_summary=result_summary or "SQL check execution failed",
                failed_row_count=None,
                sample_failing_rows=[],
            )
            grouped[severity].append(issue)
            key_anomalies.append(f"{rule_name}: SQL execution failed")
            continue

        if failing_row_count > 0:
            failed_checks += 1
            issue = DQIssue(
                rule_name=rule_name,
                description=description,
                severity=severity,
                issue_type="rule_violation",
                result_summary=result_summary,
                failed_row_count=failing_row_count,
                sample_failing_rows=sample_rows,
            )
            grouped[severity].append(issue)
            key_anomalies.append(f"{rule_name}: {failing_row_count} failing row(s)")
        else:
            passed_checks += 1

    checks_executed = len(results)
    rules_generated = len(generated_rules)
    semantic_items = semantic_inconsistencies or []
    semantic_high = 0
    semantic_medium = 0
    semantic_low = 0
    for item in semantic_items:
        if not isinstance(item, dict):
            continue
        sev = _severity_bucket(str(item.get("severity", "")))
        name = str(item.get("issue_name", "")).strip()
        if name:
            key_anomalies.append(f"{name}: semantic inconsistency")
        if sev == "high":
            semantic_high += 1
        elif sev == "low":
            semantic_low += 1
        else:
            semantic_medium += 1

    key_anomalies_found = len(grouped["high"]) + len(grouped["medium"]) + len(grouped["low"]) + len(semantic_items)

    if checks_executed == 0:
        data_quality_score = None
    else:
        violation_rate = failed_checks / checks_executed
        base_score = int(round((1 - violation_rate) * 100))
        error_penalty = execution_errors * 20
        semantic_penalty = (semantic_high * 10) + (semantic_medium * 5) + (semantic_low * 2)
        data_quality_score = max(0, min(100, base_score - error_penalty - semantic_penalty))

    if not key_anomalies:
        key_anomalies = ["No anomalies detected in executed checks"]
    else:
        key_anomalies = key_anomalies[:6]

    suggested_next_steps: list[str] = []
    has_quality_issues = bool(grouped["high"] or grouped["medium"] or grouped["low"] or semantic_items)
    if checks_executed == 0:
        suggested_next_steps.append("No SQL checks were executed; review parser warnings and regenerate executable rules.")
    if execution_errors > 0:
        suggested_next_steps.append("Review failed SQL checks and regenerate rules for execution-safe SELECT queries.")
    if grouped["high"] or grouped["medium"] or grouped["low"]:
        suggested_next_steps.append("Prioritize high-severity violations and assign data owners for remediation.")
        suggested_next_steps.append("Turn stable rules into scheduled monitoring checks and alerting.")
    if semantic_items:
        suggested_next_steps.append("Validate semantic inconsistencies with domain experts before applying rule changes.")
    if not has_quality_issues:
        suggested_next_steps.append("No immediate anomalies found; expand rule coverage and increase sample size.")
    suggested_next_steps.append("After fixes, rerun /dq/analyze to validate improvement in score and anomalies.")

    summary = DQSummary(
        table_analyzed=table_name,
        rules_generated=rules_generated,
        checks_executed=checks_executed,
        passed_checks=passed_checks,
        failed_checks=failed_checks,
        execution_errors=execution_errors,
        key_anomalies_found=key_anomalies_found,
    )

    issues = DQIssuesBySeverity(
        high=grouped["high"],
        medium=grouped["medium"],
        low=grouped["low"],
    )

    return summary, data_quality_score, issues, key_anomalies, suggested_next_steps


@dq_router.get("/tables", response_model=DQTablesResponse)
def get_dq_tables(schema_name: str = "public") -> DQTablesResponse:
    try:
        tables = list_tables(schema_name=schema_name)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list tables: {type(exc).__name__}: {exc}")

    return DQTablesResponse(
        status="ok",
        schema_name=schema_name,
        tables=tables,
    )


@dq_router.post("/analyze", response_model=DQAnalyzeResponse)
def analyze_table_data_quality(req: DQAnalyzeRequest) -> DQAnalyzeResponse:
    table_name = req.table_name.strip()
    schema_name = req.schema_name.strip() or "public"

    if not table_name:
        raise HTTPException(status_code=400, detail="table_name cannot be empty")

    try:
        exists = table_exists(table_name, schema_name=schema_name)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to verify table: {type(exc).__name__}: {exc}")

    if not exists:
        raise HTTPException(status_code=404, detail=f"Table not found: {schema_name}.{table_name}")

    try:
        schema = get_table_schema(table_name, schema_name=schema_name)
        sample_rows = get_table_sample_rows(
            table_name,
            schema_name=schema_name,
            sample_size=req.sample_size,
        )
        profile = (
            get_table_profile(table_name, schema_name=schema_name)
            if req.include_profile
            else None
        )

        generated = generate_data_quality_rules(
            table_name=table_name,
            schema=schema,
            sample_rows=sample_rows,
            profile=profile,
            max_rules=req.max_rules,
        )

        semantic = (
            generate_semantic_inconsistencies(
                table_name=table_name,
                schema=schema,
                sample_rows=sample_rows,
                profile=profile,
                max_findings=req.semantic_max_findings,
            )
            if req.include_semantic_analysis
            else {"table_name": table_name, "semantic_inconsistencies": []}
        )

        checks = execute_data_quality_checks(
            generated_rules=generated,
            sample_limit=req.sample_failing_rows,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to analyze table: {type(exc).__name__}: {exc}")

    generated_rules = generated.get("rules", [])
    if not isinstance(generated_rules, list):
        generated_rules = []
    semantic_inconsistencies = semantic.get("semantic_inconsistencies", [])
    if not isinstance(semantic_inconsistencies, list):
        semantic_inconsistencies = []

    summary, data_quality_score, issues_by_severity, key_anomalies, suggested_next_steps = (
        _build_analysis_sections(
            table_name=table_name,
            generated_rules=generated_rules,
            checks=checks,
            semantic_inconsistencies=semantic_inconsistencies,
        )
    )

    return DQAnalyzeResponse(
        status="ok",
        summary=summary,
        data_quality_score=data_quality_score,
        issues_by_severity=issues_by_severity,
        key_anomalies=key_anomalies,
        suggested_next_steps=suggested_next_steps,
        table_name=table_name,
        schema_name=schema_name,
        schema_info=DQSchemaResponse(
            schema_name=str(schema.get("schema", "")),
            table=str(schema.get("table", "")),
            columns=[DQColumnSchema.model_validate(col) for col in schema.get("columns", [])],
        ),
        sample_rows=sample_rows,
        profile=(
            DQProfileResponse(
                schema_name=str(profile.get("schema", "")),
                table=str(profile.get("table", "")),
                row_count=int(profile.get("row_count", 0)),
                column_stats=[
                    DQColumnProfileStat.model_validate(item)
                    for item in profile.get("column_stats", [])
                ],
            )
            if profile
            else None
        ),
        semantic_inconsistencies=[
            DQSemanticInconsistency.model_validate(item)
            for item in semantic_inconsistencies
        ],
        generated_rules=[DQRule.model_validate(rule) for rule in generated_rules],
        checks=DQCheckExecutionSummary.model_validate(checks),
        parser_warning=generated.get("parser_warning"),
        semantic_parser_warning=semantic.get("semantic_parser_warning"),
    )


@dq_router.post("/seed", response_model=DQSeedResponse)
def seed_dq_dataset() -> DQSeedResponse:
    try:
        result = seed_demo_dataset()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to seed demo dataset: {type(exc).__name__}: {exc}")

    return DQSeedResponse(
        status="ok",
        seed_file=str(result["seed_file"]),
        statements_executed=int(result["statements_executed"]),
        message="Data quality demo dataset seeded",
    )
