# BidManager Frontend/Backend Split

This repo now includes a starter split architecture:

- `backend/`: FastAPI service with API-key authentication.
- `frontend/`: client utilities for a desktop app to call backend jobs.

## 1) Backend responsibilities

Backend runs heavy operations:

- Browser automation (Selenium/Firefox)
- Scraping tender data
- Downloading files
- Captcha handling workflow

The backend exposes asynchronous jobs:

- `POST /v1/jobs`
- `GET /v1/jobs/{job_id}`
- `POST /v1/jobs/{job_id}/captcha`
- `GET /v1/jobs/{job_id}/artifact`

API keys are required in header `X-API-Key`.

## 2) Frontend responsibilities

Desktop frontend should keep:

- UI only
- Local settings in `%APPDATA%\\BidManager`
- Local downloads in `%LOCALAPPDATA%\\BidManager\\downloads`

Frontend sends processing requests to backend and then downloads `artifact` zip from backend and extracts locally.

## 3) Captcha flow

1. Frontend starts a backend job.
2. If captcha is required, job state becomes `captcha_required` and returns image bytes as Base64.
3. Frontend shows captcha image to user.
4. Frontend submits captcha text to backend.
5. Backend resumes browser session and continues processing.

## 4) Current design constraints

- `app_core.py` uses process-wide globals/queues, so backend executes one scraper job at a time.
- Multi-tenant + high parallel load needs a deeper refactor in `app_core.py` (session and queue isolation).

## 5) Local integration in existing PySide app

Use `frontend/api_client.py` and `frontend/remote_worker.py`:

- connect your existing captcha dialog callback
- call backend actions (`fetch_tenders`, `download_tenders`, etc.)
- extract returned artifact zip into local download folder

Actions currently mapped in backend:

- `fetch_organisations`
- `fetch_tenders`
- `download_tenders`
- `download_results`
- `check_status`
- `single_download`
