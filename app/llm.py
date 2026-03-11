from __future__ import annotations

import httpx

from app.settings import settings


def generate_answer(prompt: str) -> str:
    payload = {
        "model": settings.chat_model,
        "prompt": prompt,
        "stream": False,
    }

    with httpx.Client(timeout=240.0) as client:
        resp = client.post(f"{settings.ollama_url}/api/generate", json=payload)
        resp.raise_for_status()
        data = resp.json()

    return data.get("response", "").strip()
