# Frontend Client Helpers

## Install

```powershell
pip install -r frontend\requirements.txt
```

## Example usage

```python
from frontend.api_client import BidApiClient
from frontend.local_paths import LocalPaths
from frontend.remote_worker import run_remote_action_and_sync_downloads

paths = LocalPaths("BidManager")
paths.ensure()

client = BidApiClient(
    base_url="https://your-backend-url",
    api_key="your-api-key",
)

def captcha_dialog(image_bytes: bytes) -> str | None:
    # Connect this to your PySide6 captcha popup and return user input text.
    return None

job = run_remote_action_and_sync_downloads(
    client=client,
    action="download_tenders",
    payload={"website_id": 1},
    local_download_root=str(paths.downloads_dir),
    captcha_dialog=captcha_dialog,
)
print(job["status"], job["result"])
```

## Local data locations (Windows)

- Settings: `%APPDATA%\\BidManager\\frontend_settings.json`
- Downloads: `%LOCALAPPDATA%\\BidManager\\downloads`
