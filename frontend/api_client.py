from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from typing import Any, Callable

import requests


CaptchaSolver = Callable[[dict[str, Any]], str | None]


@dataclass
class BidApiClient:
    base_url: str
    api_key: str
    timeout_seconds: int = 30

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": self.api_key})

    def health(self) -> dict[str, Any]:
        return self._request("GET", "/v1/health")

    def create_job(self, action: str, payload: dict[str, Any], build_artifact: bool = True) -> dict[str, Any]:
        body = {"action": action, "payload": payload, "build_artifact": build_artifact}
        return self._request("POST", "/v1/jobs", json=body)

    def get_job(self, job_id: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/jobs/{job_id}")

    def submit_captcha(self, job_id: str, challenge_id: str, value: str) -> dict[str, Any]:
        body = {"challenge_id": challenge_id, "value": value}
        return self._request("POST", f"/v1/jobs/{job_id}/captcha", json=body)

    def download_artifact(self, job_id: str, output_zip: str) -> None:
        url = f"{self.base_url}/v1/jobs/{job_id}/artifact"
        with self.session.get(url, timeout=self.timeout_seconds, stream=True) as resp:
            resp.raise_for_status()
            with open(output_zip, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

    def run_job_until_done(
        self,
        action: str,
        payload: dict[str, Any],
        captcha_solver: CaptchaSolver | None = None,
        poll_interval_seconds: float = 1.2,
    ) -> dict[str, Any]:
        job = self.create_job(action=action, payload=payload)
        job_id = str(job["job_id"])
        while True:
            job = self.get_job(job_id)
            status = str(job.get("status", ""))

            if status == "captcha_required":
                cap = job.get("captcha") or {}
                challenge_id = str(cap.get("challenge_id", ""))
                if not challenge_id:
                    raise RuntimeError("Captcha is required but challenge_id is missing.")
                if captcha_solver is None:
                    raise RuntimeError("Captcha solver callback is required.")
                answer = captcha_solver(cap)
                if not answer:
                    raise RuntimeError("Captcha was cancelled by user.")
                self.submit_captcha(job_id=job_id, challenge_id=challenge_id, value=answer)

            if status == "failed":
                raise RuntimeError(str(job.get("error") or "Remote job failed."))
            if status == "completed":
                return job
            time.sleep(poll_interval_seconds)

    @staticmethod
    def decode_captcha_image(captcha_payload: dict[str, Any]) -> bytes:
        return base64.b64decode(str(captcha_payload.get("image_base64") or ""))

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        resp = self.session.request(method, url, timeout=self.timeout_seconds, **kwargs)
        resp.raise_for_status()
        return resp.json()

