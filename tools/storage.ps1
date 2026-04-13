$PSScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Definition
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$configFile = Join-Path $projectRoot "config.json"

Write-Host "============================================================="
Write-Host "    MOVIENDO ARCHIVOS PROCESADOS A LA UNIDAD DE RED"
Write-Host "============================================================="

if (-not (Test-Path $configFile)) {
    Write-Error "[FATAL] Archivo config.json no encontrado."
    exit 1
}

$config = Get-Content $configFile -Raw -Encoding UTF8 | ConvertFrom-Json

foreach ($plant in $config.plantas) {
    $plantId = $plant.id
    $destPath = $plant.storage_dest
    $sourcePath = Join-Path $projectRoot "data\$plantId\archive"

    if (-not $destPath) {
        Write-Host "[SALTANDO] $($plant.nombre): No tiene 'storage_dest' definido." -ForegroundColor Yellow
        continue
    }

    if (-not (Test-Path $sourcePath)) {
        Write-Host "  -> $($plant.nombre): Carpeta archive vacia o inexistente."
        continue
    }

    Write-Host "`n---> Procesando almacenamiento para $($plant.nombre)..." -ForegroundColor Cyan

    # Iterar sobre las carpetas de fecha (ej: 2026-03-16)
    Get-ChildItem -Path $sourcePath -Directory | ForEach-Object {
        $folderName = $_.Name
        $sourceFolder = $_.FullName
        $targetFolder = Join-Path $destPath $folderName

        try {
            Write-Host "     Copiando $folderName..."
            Copy-Item -Path $sourceFolder -Destination $targetFolder -Recurse -Force

            # VALIDAR: SI SE COPIO CORRECTAMENTE -> BORRAR ORIGEN
            if (Test-Path $targetFolder) {
                $destContent = Get-ChildItem -Path $targetFolder -Recurse -ErrorAction SilentlyContinue
                if ($destContent.Count -gt 0) {
                    Remove-Item -Path $sourceFolder -Recurse -Force
                    Write-Host "     [OK] Copiado exitoso. Origen local borrado." -ForegroundColor Green
                } else {
                    Write-Warning "     Copia vacía detectada. NO se elimina el origen."
                }
            } else {
                Write-Warning "     Destino no encontrado tras copiar. NO se borra el origen."
            }
        } catch {
            Write-Error "     [ERROR] Fallo moviendo '$folderName': $_"
        }
    }
}
Write-Host "`n============================================================="
Write-Host "        Proceso de almacenamiento completado."
Write-Host "============================================================="