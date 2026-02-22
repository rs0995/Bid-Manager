from __future__ import annotations

import os
from typing import Any

from fastapi import Header, HTTPException, status

from .key_store import ApiKeyStore


_store: ApiKeyStore | None = None


def get_store() -> ApiKeyStore:
    global _store
    if _store is None:
        _store = ApiKeyStore(server_data_dir=os.getenv("SERVER_DATA_DIR", "./server_data"))
    return _store


def require_api_key(x_api_key: str | None = Header(default=None)) -> dict[str, Any]:
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key.",
        )
    rec = get_store().validate(x_api_key)
    if not rec:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key.",
        )
    return rec


def require_admin_key(x_admin_key: str | None = Header(default=None)) -> None:
    expected = str(os.getenv("ADMIN_API_KEY", "") or "").strip()
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ADMIN_API_KEY is not configured.",
        )
    if not x_admin_key or x_admin_key != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid admin key.",
        )

