# FluxGate Local Cluster Launcher (PowerShell)
param(
    [switch]$BuildFirst = $true
)

$ErrorActionPreference = "Stop"

if ($BuildFirst) {
    Write-Host "Building FluxGate binaries..." -ForegroundColor Cyan
    go build -o bin/fluxgate-allocator.exe ./cmd/fluxgate-allocator
    go build -o bin/fluxgate-node.exe ./cmd/fluxgate-node
}

Write-Host "Starting Quota Allocator..." -ForegroundColor Green
$allocatorProc = Start-Process -FilePath "./bin/fluxgate-allocator.exe" -ArgumentList "-config configs/allocator.yaml" -PassThru -NoNewWindow
Start-Sleep -Milliseconds 800

Write-Host "Starting Node 1 (port 8081)..." -ForegroundColor Green
$node1Proc = Start-Process -FilePath "./bin/fluxgate-node.exe" -ArgumentList "-config configs/node1.yaml" -PassThru -NoNewWindow

Write-Host "Starting Node 2 (port 8082)..." -ForegroundColor Green
$node2Proc = Start-Process -FilePath "./bin/fluxgate-node.exe" -ArgumentList "-config configs/node2.yaml" -PassThru -NoNewWindow

Write-Host "Starting Node 3 (port 8083)..." -ForegroundColor Green
$node3Proc = Start-Process -FilePath "./bin/fluxgate-node.exe" -ArgumentList "-config configs/node3.yaml" -PassThru -NoNewWindow

# Save PIDs
@{
    Allocator = $allocatorProc.Id
    Node1 = $node1Proc.Id
    Node2 = $node2Proc.Id
    Node3 = $node3Proc.Id
} | ConvertTo-Json | Out-File -FilePath ".cluster-pids.json" -Encoding utf8

Write-Host "FluxGate 3-Node Cluster running successfully!" -ForegroundColor Cyan
Write-Host "Endpoints:"
Write-Host "  Allocator: gRPC 50050, Metrics: http://localhost:8050/status"
Write-Host "  Node 1: http://localhost:8081/api/v1/check"
Write-Host "  Node 2: http://localhost:8082/api/v1/check"
Write-Host "  Node 3: http://localhost:8083/api/v1/check"
Write-Host "Run scripts/stop-cluster.ps1 to terminate."
