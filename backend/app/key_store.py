from __future__ import annotations

import hashlib
import json
import os
import secrets
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ApiKeyStore:
    def __init__(self, server_data_dir: str) -> None:
        self.root = Path(server_data_dir).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.file = self.root / "api_keys.json"
        self._lock = threading.Lock()
        self._bootstrap()

    @staticmethod
    def _hash(secret: str) -> str:
        return hashlib.sha256(secret.encode("utf-8")).hexdigest()

    @staticmethod
    def _prefix(secret: str) -> str:
        return secret[:10]

    def _read(self) -> dict:
        if not self.file.exists():
            return {"keys": {}}
        try:
            return json.loads(self.file.read_text(encoding="utf-8"))
        except Exception:
            return {"keys": {}}

    def _write(self, payload: dict) -> None:
        self.file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _bootstrap(self) -> None:
        with self._lock:
            data = self._read()
            keys = data.get("keys", {})
            raw_env = str(os.getenv("APP_API_KEYS", "") or "")
            initial = [k.strip() for k in raw_env.split(",") if k.strip()]
            changed = False
            existing_hashes = {v.get("hash") for v in keys.values()}
            for secret in initial:
                h = self._hash(secret)
                if h in existing_hashes:
                    continue
                key_id = str(uuid.uuid4())
                keys[key_id] = {
                    "id": key_id,
                    "label": "env-seeded",
                    "prefix": self._prefix(secret),
                    "hash": h,
                    "created_at": _utc_now(),
                    "revoked_at": None,
                    "last_used_at": None,
                }
                changed = True
            if changed or not self.file.exists():
                data["keys"] = keys
                self._write(data)

    def validate(self, secret: str) -> dict | None:
        h = self._hash(secret)
        with self._lock:
            data = self._read()
            keys = data.get("keys", {})
            for item in keys.values():
                if item.get("revoked_at"):
                    continue
                if item.get("hash") == h:
                    item["last_used_at"] = _utc_now()
                    data["keys"] = keys
                    self._write(data)
                    return {
                        "key_id": item.get("id"),
                        "label": item.get("label"),
                        "prefix": item.get("prefix"),
                    }
        return None

    def issue(self, label: str) -> dict:
        secret = f"bm_{secrets.token_urlsafe(24)}"
        key_id = str(uuid.uuid4())
        rec = {
            "id": key_id,
            "label": str(label or "").strip() or "client",
            "prefix": self._prefix(secret),
            "hash": self._hash(secret),
            "created_at": _utc_now(),
            "revoked_at": None,
            "last_used_at": None,
        }
        with self._lock:
            data = self._read()
            keys = data.get("keys", {})
            keys[key_id] = rec
            data["keys"] = keys
            self._write(data)
        return {"key_id": key_id, "api_key": secret, "label": rec["label"], "prefix": rec["prefix"]}

    def list_keys(self) -> list[dict]:
        with self._lock:
            data = self._read()
            out = []
            for item in data.get("keys", {}).values():
                out.append(
                    {
                        "key_id": item.get("id"),
                        "label": item.get("label"),
                        "prefix": item.get("prefix"),
                        "created_at": item.get("created_at"),
                        "revoked_at": item.get("revoked_at"),
                        "last_used_at": item.get("last_used_at"),
                    }
                )
            out.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
            return out

    def revoke(self, key_id: str) -> bool:
        with self._lock:
            data = self._read()
            keys = data.get("keys", {})
            item = keys.get(key_id)
            if not item:
                return False
            if not item.get("revoked_at"):
                item["revoked_at"] = _utc_now()
                data["keys"] = keys
                self._write(data)
            return True

    def rotate(self, key_id: str) -> dict | None:
        with self._lock:
            data = self._read()
            keys = data.get("keys", {})
            item = keys.get(key_id)
            if not item:
                return None
            label = str(item.get("label") or "client")
            if not item.get("revoked_at"):
                item["revoked_at"] = _utc_now()
                keys[key_id] = item
                data["keys"] = keys
                self._write(data)
        return self.issue(label=label)

