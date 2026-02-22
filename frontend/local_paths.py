from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class LocalPaths:
    app_name: str = "BidManager"

    @property
    def config_dir(self) -> Path:
        base = Path(os.getenv("APPDATA") or Path.home())
        return base / self.app_name

    @property
    def data_dir(self) -> Path:
        base = Path(os.getenv("LOCALAPPDATA") or Path.home())
        return base / self.app_name

    @property
    def settings_file(self) -> Path:
        return self.config_dir / "frontend_settings.json"

    @property
    def downloads_dir(self) -> Path:
        return self.data_dir / "downloads"

    def ensure(self) -> None:
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.downloads_dir.mkdir(parents=True, exist_ok=True)

    def load_settings(self) -> dict:
        self.ensure()
        if not self.settings_file.exists():
            return {}
        try:
            return json.loads(self.settings_file.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def save_settings(self, payload: dict) -> None:
        self.ensure()
        self.settings_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

