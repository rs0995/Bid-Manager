from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


JobAction = Literal[
    "sync_state",
    "fetch_organisations",
    "fetch_tenders",
    "download_tenders",
    "download_results",
    "check_status",
    "single_download",
    "deliver_tender_docs",
]


class JobCreateRequest(BaseModel):
    action: JobAction
    payload: dict[str, Any] = Field(default_factory=dict)
    build_artifact: bool = True


class CaptchaSubmitRequest(BaseModel):
    challenge_id: str
    value: str


class ApiKeyIssueRequest(BaseModel):
    label: str = "client"


class JobView(BaseModel):
    job_id: str
    action: JobAction
    status: Literal["queued", "running", "captcha_required", "completed", "failed"]
    created_at: datetime
    updated_at: datetime
    payload: dict[str, Any]
    result: dict[str, Any] | None = None
    error: str | None = None
    logs: list[str] = Field(default_factory=list)
    captcha: dict[str, Any] | None = None
