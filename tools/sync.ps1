# =============================================================
# SCRIPT DE SINCRONIZACIÓN GLOBAL INTELIGENTE (sync.ps1)
# =============================================================
$PSScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Definition
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$configFile = Join-Path $projectRoot "config.json"

$config = Get-Content $configFile -Raw -Encoding UTF8 | ConvertFrom-Json

foreach ($plant in $config.plantas) {
    $plantId = $plant.id
    $srcPath = $plant.sync_src
    $dstPath = Join-Path $projectRoot "data\$plantId\input"
    $historyFile = Join-Path $projectRoot "config\plants\$plantId\processed_files.json"

    Write-Host "`n---> Sincronizando $($plant.nombre)..." -ForegroundColor Cyan
    
    # Lista de exclusión dinámica
    $excludeList = @("*.json", "*.ini")
    if (Test-Path $historyFile) {
        $processed = Get-Content $historyFile -Raw -Encoding UTF8 | ConvertFrom-Json
        foreach ($file in $processed) { 
            $name = [System.IO.Path]::GetFileName($file)
            $excludeList += "`"$name`"" 
        }
    }

    if (-not (Test-Path $dstPath)) { New-Item -ItemType Directory -Force -Path $dstPath | Out-Null }

    $robocopyArgs = @("`"$srcPath`"", "`"$dstPath`"", "/E", "/XO", "/R:1", "/W:2", "/NP", "/XF") + $excludeList
    Start-Process robocopy -ArgumentList $robocopyArgs -Wait -NoNewWindow
}