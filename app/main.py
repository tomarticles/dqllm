from __future__ import annotations

from fastapi import FastAPI

from app.data_quality.api import dq_router


app = FastAPI(title="Data Quality API", version="1.0.0")
app.include_router(dq_router)


@app.get("/")
def root() -> dict[str, str]:
    return {"status": "ok", "message": "Data Quality API is running"}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "healthy"}
