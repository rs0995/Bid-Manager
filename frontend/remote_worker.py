from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Any, Callable

from .api_client import BidApiClient


CaptchaDialog = Callable[[bytes], str | None]


def run_remote_action_and_sync_downloads(
    client: BidApiClient,
    action: str,
    payload: dict[str, Any],
    local_download_root: str,
    captcha_dialog: CaptchaDialog,
) -> dict[str, Any]:
    local_root = Path(local_download_root)
    local_root.mkdir(parents=True, exist_ok=True)

    def solve(captcha_payload: dict[str, Any]) -> str | None:
        image_bytes = BidApiClient.decode_captcha_image(captcha_payload)
        return captcha_dialog(image_bytes)

    job = client.run_job_until_done(action=action, payload=payload, captcha_solver=solve)
    job_id = str(job["job_id"])
    result = job.get("result") or {}
    if bool(result.get("artifact_available", False)):
        raw_zip = io.BytesIO()
        tmp_zip = local_root / f"{job_id}.zip"
        client.download_artifact(job_id, str(tmp_zip))
        raw_zip.write(tmp_zip.read_bytes())
        tmp_zip.unlink(missing_ok=True)
        raw_zip.seek(0)
        with zipfile.ZipFile(raw_zip, "r") as zf:
            zf.extractall(local_root)
    return job

