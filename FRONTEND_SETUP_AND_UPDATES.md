# Frontend Setup + Server Update Flow

## 1) Build fast-startup setup

Use onedir installer build (faster startup than onefile):

```powershell
powershell -ExecutionPolicy Bypass -File scripts/build_frontend_setup.ps1
```

Output:
- Setup EXE in `deploy/` (e.g. `BidManagerSetup_<version>.exe`)

## 2) Publish update manifest for server-hosted updates

After uploading setup file(s) to your server/static hosting, publish manifest:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/publish_frontend_update.ps1 `
  -DeployDir deploy `
  -PublicBaseUrl "https://your-domain.com/updates"
```

This writes `deploy/build_version.json` with fields used by the app updater:
- `version`
- `installer_url`
- optional `exe_url`

Upload `build_version.json` to:
- `https://your-domain.com/updates/build_version.json`

## 3) Configure frontend app

In app `Settings`:
- set `Update Manifest URL` to:
  - `https://your-domain.com/updates/build_version.json`

`Check for Upgrade` now checks remote first, then local update folder fallback.

If update is available:
- if `installer_url` exists: app downloads installer and launches it
- else if `exe_url` exists: app downloads EXE and uses self-update replace flow
