from __future__ import annotations

import base64
import os
import queue
import shutil
import threading
import uuid
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import app_core as core

from .models import JobAction, JobView, utcnow


def _safe_key_fragment(api_key: str) -> str:
    return "".join(ch for ch in api_key if ch.isalnum())[:12] or "client"


def _list_files_with_meta(root: Path) -> dict[str, tuple[int, int]]:
    out: dict[str, tuple[int, int]] = {}
    if not root.exists():
        return out
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        rel = str(p.relative_to(root)).replace("\\", "/")
        st = p.stat()
        out[rel] = (int(st.st_mtime_ns), int(st.st_size))
    return out


def _build_changed_artifact(
    job_id: str,
    before: dict[str, tuple[int, int]],
    after: dict[str, tuple[int, int]],
    download_root: Path,
    db_path: Path,
    artifact_dir: Path,
    force_include_prefixes: list[str] | None = None,
) -> tuple[Path | None, int]:
    changed = [rel for rel, meta in after.items() if before.get(rel) != meta]
    prefixes = [str(x or "").strip().replace("\\", "/").strip("/") for x in (force_include_prefixes or []) if str(x or "").strip()]
    if prefixes:
        forced = [
            rel for rel in after
            if any(rel == prefix or rel.startswith(prefix + "/") for prefix in prefixes)
        ]
        changed = sorted(set(changed).union(forced))
    artifact_dir.mkdir(parents=True, exist_ok=True)
    zip_path = artifact_dir / f"{job_id}.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for rel in changed:
            abs_path = download_root / rel
            if abs_path.exists() and abs_path.is_file():
                zf.write(abs_path, arcname=rel)
        # Always return a DB snapshot so frontend can sync local UI state.
        if db_path.exists() and db_path.is_file():
            zf.write(db_path, arcname="__state/tender_manager.db")
    if not changed and (not db_path.exists()):
        return None, 0
    return zip_path, len(changed)


@dataclass
class JobState:
    job_id: str
    action: JobAction
    payload: dict[str, Any]
    build_artifact: bool
    status: str = "queued"
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)
    result: dict[str, Any] | None = None
    error: str | None = None
    logs: list[str] = field(default_factory=list)
    captcha: dict[str, Any] | None = None
    pending_challenge_id: str | None = None
    pending_answer: queue.Queue[str | None] = field(default_factory=queue.Queue)
    artifact_path: Path | None = None

    def to_view(self) -> JobView:
        return JobView(
            job_id=self.job_id,
            action=self.action,
            status=self.status,  # type: ignore[arg-type]
            created_at=self.created_at,
            updated_at=self.updated_at,
            payload=self.payload,
            result=self.result,
            error=self.error,
            logs=self.logs[-400:],
            captcha=self.captcha,
        )


class JobManager:
    def __init__(self, server_data_dir: str, captcha_timeout_seconds: int = 300) -> None:
        self.server_data_dir = Path(server_data_dir).resolve()
        self.server_data_dir.mkdir(parents=True, exist_ok=True)
        self.captcha_timeout_seconds = int(captcha_timeout_seconds)
        self._jobs: dict[str, JobState] = {}
        self._lock = threading.Lock()
        # app_core uses process-wide globals/queues, so keep one scraping job at a time.
        self._worker_lock = threading.Lock()

    def create_job(self, action: JobAction, payload: dict[str, Any], build_artifact: bool) -> JobView:
        job_id = str(uuid.uuid4())
        st = JobState(job_id=job_id, action=action, payload=payload, build_artifact=build_artifact)
        with self._lock:
            self._jobs[job_id] = st
        t = threading.Thread(target=self._run_job, args=(st,), daemon=True)
        t.start()
        return st.to_view()

    def get_job(self, job_id: str) -> JobState | None:
        with self._lock:
            return self._jobs.get(job_id)

    def submit_captcha(self, job_id: str, challenge_id: str, value: str) -> bool:
        job = self.get_job(job_id)
        if not job:
            return False
        with self._lock:
            if job.pending_challenge_id != challenge_id:
                return False
            job.pending_answer.put(value.strip())
            return True

    def get_artifact_path(self, job_id: str) -> Path | None:
        job = self.get_job(job_id)
        if not job:
            return None
        return job.artifact_path

    def _run_job(self, job: JobState) -> None:
        bridge_stop: threading.Event | None = None
        log_stop: threading.Event | None = None
        bridge: threading.Thread | None = None
        log_thread: threading.Thread | None = None
        try:
            with self._worker_lock:
                self._set_status(job, "running")
                user_root = self.server_data_dir / _safe_key_fragment(str(job.payload.get("_api_key_id", "")))
                user_db = user_root / "tender_manager.db"
                user_projects = user_root / "projects"
                user_downloads = user_root / "downloads"
                user_templates = user_root / "templates"
                for p in (user_projects, user_downloads, user_templates):
                    p.mkdir(parents=True, exist_ok=True)

                core.save_app_paths_config(
                    db_file=str(user_db),
                    root_folder=str(user_projects),
                    download_folder=str(user_downloads),
                    template_folder=str(user_templates),
                )
                core.DB_FILE = str(user_db)
                core.ROOT_FOLDER = str(user_projects)
                core.BASE_DOWNLOAD_DIRECTORY = str(user_downloads)
                core.TEMPLATE_LIBRARY_FOLDER = str(user_templates)
                incoming_db_b64 = str(job.payload.get("db_snapshot_base64") or "").strip()
                if incoming_db_b64:
                    raw_db = base64.b64decode(incoming_db_b64.encode("ascii"))
                    user_db.write_bytes(raw_db)
                core.init_db()

                before = _list_files_with_meta(user_downloads)
                bridge_stop = threading.Event()
                bridge = threading.Thread(
                    target=self._captcha_bridge_worker,
                    args=(job, bridge_stop),
                    daemon=True,
                )
                bridge.start()

                log_stop = threading.Event()
                log_thread = threading.Thread(target=self._log_pump_worker, args=(job, log_stop), daemon=True)
                log_thread.start()

                self._execute_action(job)
                after = _list_files_with_meta(user_downloads)
                changed_count = 0
                if job.build_artifact:
                    artifact_dir = user_root / "artifacts"
                    force_prefixes = job.payload.get("_artifact_include_prefixes") or []
                    artifact, changed_count = _build_changed_artifact(
                        job_id=job.job_id,
                        before=before,
                        after=after,
                        download_root=user_downloads,
                        db_path=user_db,
                        artifact_dir=artifact_dir,
                        force_include_prefixes=force_prefixes if isinstance(force_prefixes, list) else None,
                    )
                    job.artifact_path = artifact

                job.result = {
                    "db_file": str(user_db),
                    "download_root": str(user_downloads),
                    "changed_files": changed_count,
                    "artifact_available": bool(job.artifact_path and job.artifact_path.exists()),
                }
                self._set_status(job, "completed")
        except Exception as exc:
            job.error = str(exc)
            self._append_log(job, f"Job failed: {exc}")
            self._set_status(job, "failed")
        finally:
            if bridge_stop is not None:
                bridge_stop.set()
            if log_stop is not None:
                log_stop.set()
            if bridge is not None:
                bridge.join(timeout=1.5)
            if log_thread is not None:
                log_thread.join(timeout=1.5)
            with self._lock:
                job.pending_challenge_id = None
                job.captcha = None

    def _set_status(self, job: JobState, status_txt: str) -> None:
        with self._lock:
            job.status = status_txt
            job.updated_at = utcnow()

    def _append_log(self, job: JobState, text: str) -> None:
        with self._lock:
            stamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
            job.logs.append(f"[{stamp}Z] {text}")
            job.updated_at = utcnow()

    def _log_pump_worker(self, job: JobState, stop_evt: threading.Event) -> None:
        while not stop_evt.is_set():
            try:
                msg = core.log_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            self._append_log(job, str(msg))

    def _captcha_bridge_worker(self, job: JobState, stop_evt: threading.Event) -> None:
        while not stop_evt.is_set():
            try:
                img_data = core.captcha_req_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            challenge_id = str(uuid.uuid4())
            expires_at = (utcnow() + timedelta(seconds=self.captcha_timeout_seconds)).isoformat()
            image_b64 = base64.b64encode(img_data).decode("ascii")
            with self._lock:
                job.pending_challenge_id = challenge_id
                job.captcha = {
                    "challenge_id": challenge_id,
                    "image_base64": image_b64,
                    "expires_at_utc": expires_at,
                }
                job.status = "captcha_required"
                job.updated_at = utcnow()

            answer: str | None = None
            deadline = utcnow() + timedelta(seconds=self.captcha_timeout_seconds)
            while utcnow() < deadline and not stop_evt.is_set():
                try:
                    answer = job.pending_answer.get(timeout=0.25)
                    break
                except queue.Empty:
                    continue

            if not answer:
                core.captcha_res_queue.put(None)
                self._append_log(job, "Captcha timed out or was cancelled.")
            else:
                core.captcha_res_queue.put(answer)
                self._append_log(job, "Captcha submitted by client.")

            with self._lock:
                job.pending_challenge_id = None
                job.captcha = None
                if job.status == "captcha_required":
                    job.status = "running"
                job.updated_at = utcnow()

    def _execute_action(self, job: JobState) -> None:
        payload = dict(job.payload)
        payload.pop("_api_key", None)
        action = job.action

        if action == "sync_state":
            return
        if action == "fetch_organisations":
            core.ScraperBackend.fetch_organisations_logic(int(payload["website_id"]))
            return
        if action == "fetch_tenders":
            core.ScraperBackend.fetch_tenders_logic(int(payload["website_id"]))
            return
        if action == "download_tenders":
            target_ids = payload.get("target_db_ids")
            forced_mode = payload.get("forced_mode")
            core.ScraperBackend.download_tenders_logic(
                int(payload["website_id"]),
                target_db_ids=target_ids,
                forced_mode=forced_mode,
            )
            return
        if action == "download_results":
            core.ScraperBackend.download_tender_results_logic(int(payload["website_id"]))
            return
        if action == "check_status":
            core.ScraperBackend.check_tender_status_logic(
                int(payload["website_id"]),
                archived_only=bool(payload.get("archived_only", False)),
            )
            return
        if action == "single_download":
            core.ScraperBackend.download_single_tender_logic(
                int(payload["tender_db_id"]),
                str(payload["mode"]),
            )
            return
        if action == "deliver_tender_docs":
            tender_id = str(payload.get("source_tender_id") or "").strip()
            mode = str(payload.get("mode") or "full").strip().lower()
            if mode not in {"full", "update"}:
                mode = "full"
            if not tender_id:
                raise ValueError("source_tender_id is required.")

            conn = core.sqlite3.connect(core.DB_FILE)
            try:
                row = conn.execute(
                    "SELECT id, COALESCE(folder_path,'') FROM tenders "
                    "WHERE TRIM(COALESCE(tender_id,''))=? AND COALESCE(is_archived,0)=0 "
                    "ORDER BY id DESC LIMIT 1",
                    (tender_id,),
                ).fetchone()
            finally:
                conn.close()
            if not row:
                raise ValueError(f"Tender not found for id '{tender_id}'.")

            tender_db_id = int(row[0])
            safe_id = core.re.sub(r'[\\/*?:"<>|]', "", tender_id)
            preferred_dir = Path(core.BASE_DOWNLOAD_DIRECTORY) / safe_id
            existing_folder = str(row[1] or "").strip()
            existing_path = Path(existing_folder) if existing_folder else None
            target_dir = existing_path if existing_path and existing_path.is_dir() else preferred_dir
            target_dir.mkdir(parents=True, exist_ok=True)

            has_existing_files = any(p.is_file() for p in target_dir.rglob("*"))
            if mode == "full":
                if not has_existing_files:
                    core.ScraperBackend.download_single_tender_logic(tender_db_id, "full")
            else:
                core.ScraperBackend.download_single_tender_logic(tender_db_id, "update")

            try:
                rel_prefix = str(target_dir.relative_to(Path(core.BASE_DOWNLOAD_DIRECTORY))).replace("\\", "/")
            except Exception:
                rel_prefix = safe_id
            job.payload["_artifact_include_prefixes"] = [rel_prefix]
            return
        raise ValueError(f"Unsupported action: {action}")
