param(
    [string]$EntryScript = "bid_frontend_client.py",
    [string]$AppName = "BidManager",
    [string]$BuildVersion = "",
    [switch]$Clean
)

$ErrorActionPreference = "Stop"

Write-Host "Building fast-startup frontend setup (onedir + installer)..."
$argsList = @(
    "-ExecutionPolicy", "Bypass",
    "-File", "scripts/build_installer.ps1",
    "-EntryScript", $EntryScript,
    "-AppName", $AppName
)
if ($BuildVersion) {
    $argsList += @("-BuildVersion", $BuildVersion)
}
if ($Clean) {
    $argsList += "-Clean"
}

& powershell @argsList
if ($LASTEXITCODE -ne 0) {
    throw "Frontend setup build failed with exit code $LASTEXITCODE"
}

Write-Host "Frontend setup build complete."
