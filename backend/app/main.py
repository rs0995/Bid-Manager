from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse

from .auth import get_store, require_admin_key, require_api_key
from .job_manager import JobManager
from .models import ApiKeyIssueRequest, CaptchaSubmitRequest, JobCreateRequest, JobView

load_dotenv()

app = FastAPI(title="BidManager Backend", version="1.0.0")
server_data_dir = os.getenv("SERVER_DATA_DIR", "./server_data")

manager = JobManager(
    server_data_dir=server_data_dir,
    captcha_timeout_seconds=int(os.getenv("CAPTCHA_TIMEOUT_SECONDS", "300")),
)
api_store = get_store()


def _request_log_path() -> Path:
    p = Path(server_data_dir).resolve()
    p.mkdir(parents=True, exist_ok=True)
    return p / "request_logs.jsonl"


@app.middleware("http")
async def request_log_middleware(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    elapsed_ms = int((time.time() - start) * 1000)
    rec = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "method": request.method,
        "path": request.url.path,
        "status_code": int(response.status_code),
        "duration_ms": elapsed_ms,
        "client_ip": (request.client.host if request.client else ""),
    }
    x_api_key = request.headers.get("x-api-key")
    if x_api_key:
        info = api_store.validate(x_api_key)
        if info:
            rec["api_key_id"] = info.get("key_id")
            rec["api_key_prefix"] = info.get("prefix")
        else:
            rec["api_key_id"] = "invalid"
    with open(_request_log_path(), "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=True) + "\n")
    return response


@app.get("/v1/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/jobs", response_model=JobView)
def create_job(req: JobCreateRequest, api_key: dict = Depends(require_api_key)) -> JobView:
    payload = dict(req.payload or {})
    payload["_api_key_id"] = str(api_key.get("key_id") or "")
    return manager.create_job(action=req.action, payload=payload, build_artifact=req.build_artifact)


@app.get("/v1/jobs/{job_id}", response_model=JobView)
def get_job(job_id: str, _: dict = Depends(require_api_key)) -> JobView:
    job = manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    return job.to_view()


@app.post("/v1/jobs/{job_id}/captcha")
def submit_captcha(job_id: str, req: CaptchaSubmitRequest, _: dict = Depends(require_api_key)) -> dict[str, bool]:
    ok = manager.submit_captcha(job_id=job_id, challenge_id=req.challenge_id, value=req.value)
    if not ok:
        raise HTTPException(status_code=400, detail="Captcha challenge mismatch or expired.")
    return {"accepted": True}


@app.get("/v1/jobs/{job_id}/artifact")
def download_artifact(job_id: str, _: dict = Depends(require_api_key)) -> FileResponse:
    artifact = manager.get_artifact_path(job_id)
    if not artifact or not artifact.exists():
        raise HTTPException(status_code=404, detail="Artifact not found for this job.")
    return FileResponse(
        path=Path(artifact),
        media_type="application/zip",
        filename=f"{job_id}.zip",
    )


@app.get("/v1/admin/keys")
def list_api_keys(_: None = Depends(require_admin_key)) -> dict:
    return {"items": api_store.list_keys()}


@app.post("/v1/admin/keys")
def issue_api_key(req: ApiKeyIssueRequest, _: None = Depends(require_admin_key)) -> dict:
    return api_store.issue(label=req.label)


@app.post("/v1/admin/keys/{key_id}/rotate")
def rotate_api_key(key_id: str, _: None = Depends(require_admin_key)) -> dict:
    rotated = api_store.rotate(key_id)
    if not rotated:
        raise HTTPException(status_code=404, detail="API key not found.")
    return rotated


@app.post("/v1/admin/keys/{key_id}/revoke")
def revoke_api_key(key_id: str, _: None = Depends(require_admin_key)) -> dict:
    ok = api_store.revoke(key_id)
    if not ok:
        raise HTTPException(status_code=404, detail="API key not found.")
    return {"revoked": True, "key_id": key_id}
