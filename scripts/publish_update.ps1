param(
    [string]$ExePath = "dist\BidManager.exe",
    [string]$DeployDir = "deploy",
    [string]$ExeName = "BidManager.exe",
    [string]$ManifestName = "build_version.json"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $ExePath)) {
    throw "Executable not found: $ExePath. Build first with scripts/build_exe.ps1"
}

New-Item -ItemType Directory -Path $DeployDir -Force | Out-Null
$target = Join-Path $DeployDir $ExeName
try {
    Copy-Item $ExePath $target -Force
    Write-Host "Published: $target"
}
catch {
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $altName = [System.IO.Path]::GetFileNameWithoutExtension($ExeName) + "_$stamp.exe"
    $altPath = Join-Path $DeployDir $altName
    Copy-Item $ExePath $altPath -Force
    Write-Host "Primary target is locked: $target"
    Write-Host "Published fallback build: $altPath"
    Write-Host "Close the running app and replace $target when possible."
}

Write-Host "Users can launch this EXE directly or from a desktop shortcut."

$version = ""
$buildUtc = ""
if (Test-Path "app_version.py") {
    $raw = Get-Content "app_version.py" -Raw
    if ($raw -match 'APP_VERSION\s*=\s*\"([^\"]+)\"') {
        $version = $Matches[1]
    }
    if ($raw -match 'BUILD_UTC\s*=\s*\"([^\"]+)\"') {
        $buildUtc = $Matches[1]
    }
}
if (-not $version) {
    $version = Get-Date -Format "yyyy.MM.dd.HHmmss"
}
if (-not $buildUtc) {
    $buildUtc = (Get-Date).ToUniversalTime().ToString("o")
}

$manifest = [ordered]@{
    app_name = "BidManager"
    exe_name = $ExeName
    version = $version
    built_at_utc = $buildUtc
}
$manifestPath = Join-Path $DeployDir $ManifestName
$manifest | ConvertTo-Json | Set-Content -Path $manifestPath -Encoding UTF8
Write-Host "Update manifest written: $manifestPath (version $version)"
