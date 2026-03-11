# Data Quality with LLM

This folder is a standalone version of the AI-assisted Data Quality module only.

## What is included

- FastAPI API for:
  - `GET /health`
  - `GET /dq/tables`
  - `POST /dq/analyze`
  - `POST /dq/seed`
- PostgreSQL seed dataset (`customers`, `orders`, `payments`)
- Ollama-based rule generation + semantic inconsistency analysis

## Run

```bash
cd data-quality-only
docker compose up -d postgres

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## Environment

Use `.env` (already provided) or copy from `.env.example`.

## Seed / Analyze

```bash
curl -X POST http://127.0.0.1:8000/dq/seed

curl -s -X POST http://127.0.0.1:8000/dq/analyze \
  -H "Content-Type: application/json" \
  -d '{"table_name":"customers","include_profile":true,"include_semantic_analysis":true}' | jq
```
