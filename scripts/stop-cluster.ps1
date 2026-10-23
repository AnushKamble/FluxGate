# FluxGate Local Cluster Stopper (PowerShell)

if (Test-Path ".cluster-pids.json") {
    $pids = Get-Content ".cluster-pids.json" | ConvertFrom-Json
    Write-Host "Stopping FluxGate Cluster..." -ForegroundColor Yellow

    foreach ($prop in $pids.PSObject.Properties) {
        $id = $prop.Value
        Write-Host "Stopping $($prop.Name) (PID: $id)..."
        try {
            Stop-Process -Id $id -Force -ErrorAction SilentlyContinue
        } catch {
            # Ignore if already stopped
        }
    }
    Remove-Item ".cluster-pids.json" -Force
    Write-Host "FluxGate Cluster stopped." -ForegroundColor Green
} else {
    Write-Host "No active cluster PID file (.cluster-pids.json) found. Looking for processes by name..."
    Stop-Process -Name "fluxgate-node", "fluxgate-allocator" -Force -ErrorAction SilentlyContinue
    Write-Host "Processes stopped." -ForegroundColor Green
}
