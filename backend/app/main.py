from __future__ import annotations

import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse

from .auth import get_store, require_admin_key, require_api_key
from .job_manager import JobManager
from .models import (
    ApiKeyIssueRequest,
    CaptchaSubmitRequest,
    JobCreateRequest,
    JobView,
    StorageDeleteFolderRequest,
    StorageDeleteOlderRequest,
    StorageListRequest,
)

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


def _server_root() -> Path:
    p = Path(server_data_dir).resolve()
    p.mkdir(parents=True, exist_ok=True)
    return p


def _resolve_storage_path(relative_path: str) -> Path:
    base = _server_root()
    rel = str(relative_path or "").replace("\\", "/").strip().lstrip("/")
    candidate = (base / rel).resolve()
    try:
        candidate.relative_to(base)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Path is outside server storage root.") from exc
    return candidate


def _iter_storage(base_root: Path, root: Path, max_entries: int) -> tuple[list[dict], bool]:
    items: list[dict] = []
    truncated = False
    for current_root, dirs, files in os.walk(root):
        dirs.sort()
        files.sort()
        current = Path(current_root)
        rel_dir = current.relative_to(base_root)
        if rel_dir != Path("."):
            stat = current.stat()
            items.append(
                {
                    "path": rel_dir.as_posix(),
                    "name": current.name,
                    "kind": "dir",
                    "size_bytes": 0,
                    "modified_utc": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
                }
            )
            if len(items) >= max_entries:
                truncated = True
                break
        for name in files:
            file_path = current / name
            stat = file_path.stat()
            rel_path = file_path.relative_to(base_root).as_posix()
            items.append(
                {
                    "path": rel_path,
                    "name": name,
                    "kind": "file",
                    "size_bytes": int(stat.st_size),
                    "modified_utc": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
                }
            )
            if len(items) >= max_entries:
                truncated = True
                break
        if truncated:
            break
    return items, truncated


def _storage_usage(root: Path) -> dict:
    total_bytes = 0
    file_count = 0
    dir_count = 0
    latest_mtime = None
    for current_root, dirs, files in os.walk(root):
        dir_count += len(dirs)
        for name in files:
            file_path = Path(current_root) / name
            try:
                stat = file_path.stat()
            except OSError:
                continue
            file_count += 1
            total_bytes += int(stat.st_size)
            latest_mtime = max(latest_mtime or stat.st_mtime, stat.st_mtime)
    return {
        "root": root.as_posix(),
        "total_bytes": total_bytes,
        "file_count": file_count,
        "dir_count": dir_count,
        "latest_modified_utc": (
            datetime.fromtimestamp(latest_mtime, timezone.utc).isoformat() if latest_mtime else ""
        ),
    }


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


@app.get("/v1/admin/storage/usage")
def admin_storage_usage(_: None = Depends(require_admin_key)) -> dict:
    root = _server_root()
    usage = _storage_usage(root)
    return {"usage": usage}


@app.post("/v1/admin/storage/list")
def admin_storage_list(req: StorageListRequest, _: None = Depends(require_admin_key)) -> dict:
    max_entries = max(1, min(10000, int(req.max_entries or 2000)))
    root = _resolve_storage_path(req.relative_root)
    if not root.exists():
        raise HTTPException(status_code=404, detail="Storage path not found.")
    if root.is_file():
        stat = root.stat()
        item = {
            "path": root.relative_to(_server_root()).as_posix(),
            "name": root.name,
            "kind": "file",
            "size_bytes": int(stat.st_size),
            "modified_utc": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
        }
        return {"root": item["path"], "items": [item], "truncated": False}
    items, truncated = _iter_storage(_server_root(), root, max_entries)
    return {
        "root": root.relative_to(_server_root()).as_posix() if root != _server_root() else "",
        "items": items,
        "truncated": truncated,
    }


@app.post("/v1/admin/storage/delete-folder")
def admin_storage_delete_folder(req: StorageDeleteFolderRequest, _: None = Depends(require_admin_key)) -> dict:
    target = _resolve_storage_path(req.relative_path)
    base = _server_root()
    if target == base:
        raise HTTPException(status_code=400, detail="Deleting the storage root is not allowed.")
    if not target.exists():
        raise HTTPException(status_code=404, detail="Folder not found.")
    if not target.is_dir():
        raise HTTPException(status_code=400, detail="Target is not a folder.")
    shutil.rmtree(target)
    return {"deleted": True, "relative_path": target.relative_to(base).as_posix()}


@app.post("/v1/admin/storage/delete-older")
def admin_storage_delete_older(req: StorageDeleteOlderRequest, _: None = Depends(require_admin_key)) -> dict:
    root = _resolve_storage_path(req.relative_root)
    if not root.exists():
        raise HTTPException(status_code=404, detail="Storage path not found.")
    cutoff = time.time() - (int(req.days) * 86400)
    deleted_files = 0
    deleted_dirs = 0
    for current_root, dirs, files in os.walk(root, topdown=False):
        for name in files:
            file_path = Path(current_root) / name
            try:
                if file_path.stat().st_mtime < cutoff:
                    file_path.unlink()
                    deleted_files += 1
            except OSError:
                continue
        for name in dirs:
            dir_path = Path(current_root) / name
            try:
                if not any(dir_path.iterdir()):
                    dir_path.rmdir()
                    deleted_dirs += 1
            except OSError:
                continue
    return {
        "deleted_files": deleted_files,
        "deleted_empty_dirs": deleted_dirs,
        "relative_root": root.relative_to(_server_root()).as_posix() if root != _server_root() else "",
        "older_than_days": int(req.days),
    }
