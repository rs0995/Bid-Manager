# Backend Service

## Setup

1. Create venv and install:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r backend\requirements.txt
```

2. Configure env:

```powershell
copy backend\.env.example backend\.env
```

Edit `backend\.env` and set `APP_API_KEYS`.
Also set `ADMIN_API_KEY` for key issuance/rotation endpoints.

3. Run server:

```powershell
python -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000

## Docker run

```powershell
docker build -f backend\Dockerfile -t bidmanager-backend .
docker run --rm -p 8000:8000 --env-file backend\.env bidmanager-backend
```
```

## API summary

- `GET /v1/health`
- `POST /v1/jobs`
- `GET /v1/jobs/{job_id}`
- `POST /v1/jobs/{job_id}/captcha`
- `GET /v1/jobs/{job_id}/artifact`
- `GET /v1/admin/keys` (admin key required)
- `POST /v1/admin/keys` (admin key required)
- `POST /v1/admin/keys/{key_id}/rotate` (admin key required)
- `POST /v1/admin/keys/{key_id}/revoke` (admin key required)

All endpoints except `/v1/health` require `X-API-Key`.
Admin endpoints require `X-Admin-Key`.

## Example create job

```json
{
  "action": "fetch_tenders",
  "payload": { "website_id": 1 },
  "build_artifact": true
}
```

## Notes

- Server stores its own DB/download workspace in `SERVER_DATA_DIR`.
- Browser scraping runs server-side only.
- Captcha is returned to client when manual input is needed.
- Request logs are appended to `SERVER_DATA_DIR/request_logs.jsonl`.
