param(
    [string]$DeployDir = "deploy",
    [string]$PublicBaseUrl = "",
    [string]$ManifestName = "build_version.json"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $DeployDir)) {
    throw "Deploy directory not found: $DeployDir"
}

$installer = Get-ChildItem -Path $DeployDir -Filter "BidManagerSetup_*.exe" -File |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

$portable = Get-ChildItem -Path $DeployDir -Filter "BidManager_portable_*.zip" -File |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

if (-not $installer -and -not $portable) {
    throw "No installer/portable package found in $DeployDir. Run scripts/build_frontend_setup.ps1 first."
}

$manifestPathLocal = Join-Path $DeployDir "build_version_installer.json"
$version = ""
$builtUtc = ""
if (Test-Path $manifestPathLocal) {
    try {
        $meta = Get-Content $manifestPathLocal -Raw | ConvertFrom-Json
        $version = [string]$meta.version
        $builtUtc = [string]$meta.built_at_utc
    }
    catch {}
}
if (-not $version) {
    $version = Get-Date -Format "yyyy.MM.dd.HHmmss"
}
if (-not $builtUtc) {
    $builtUtc = (Get-Date).ToUniversalTime().ToString("o")
}

function Join-Url([string]$base, [string]$name) {
    if (-not $base) { return "" }
    $b = $base.TrimEnd("/")
    return "$b/$name"
}

$installerName = if ($installer) { $installer.Name } else { "" }
$portableName = if ($portable) { $portable.Name } else { "" }

$manifest = [ordered]@{
    app_name = "BidManager"
    version = $version
    built_at_utc = $builtUtc
    installer_name = $installerName
    installer_url = (Join-Url $PublicBaseUrl $installerName)
    notes = "Server-hosted update feed for frontend UI."
}

# Optional compatibility for EXE-based updater path.
$exe = Get-ChildItem -Path $DeployDir -Filter "BidManager.exe" -File | Select-Object -First 1
if ($exe) {
    $manifest["exe_name"] = $exe.Name
    $manifest["exe_url"] = Join-Url $PublicBaseUrl $exe.Name
}

$manifestPath = Join-Path $DeployDir $ManifestName
$manifest | ConvertTo-Json | Set-Content -Path $manifestPath -Encoding UTF8
Write-Host "Published update manifest: $manifestPath"
if ($PublicBaseUrl) {
    Write-Host "Manifest URL expected at: $($PublicBaseUrl.TrimEnd('/'))/$ManifestName"
}
