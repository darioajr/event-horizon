# start-eventhorizon.ps1
# Script para iniciar o eventhorizon e o Kafka UI
#
# Uso: .\start-eventhorizon.ps1

param(
    [switch]$Stop,
    [switch]$Build,
    [switch]$Help
)

if ($Help) {
    Write-Host @"
eventhorizon Startup Script
=========================

Uso:
    .\start-eventhorizon.ps1           # Iniciar eventhorizon + Kafka UI
    .\start-eventhorizon.ps1 -Build    # Recompilar e iniciar
    .\start-eventhorizon.ps1 -Stop     # Parar tudo
    .\start-eventhorizon.ps1 -Help     # Mostrar ajuda

Servicos:
    - eventhorizon: localhost:9092
    - Kafka UI:   http://localhost:9080
"@
    exit 0
}

$ErrorActionPreference = "Continue"

Write-Host ""
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "  eventhorizon" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

if ($Stop) {
    Write-Host "Parando servicos..." -ForegroundColor Yellow
    docker-compose -f docker-compose.yml down
    
    # Parar eventhorizon se estiver rodando
    $eventhorizon = Get-Process -Name "event_horizon" -ErrorAction SilentlyContinue
    if ($eventhorizon) {
        Write-Host "Parando eventhorizon..." -ForegroundColor Yellow
        Stop-Process -Name "event_horizon" -Force
    }
    
    Write-Host "Servicos parados!" -ForegroundColor Green
    exit 0
}

# Compilar se necessário
if ($Build -or -not (Test-Path ".\build\windows-release\Release\event_horizon.exe")) {
    Write-Host "Compilando eventhorizon..." -ForegroundColor Yellow
    & .\build.ps1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERRO: Falha na compilacao" -ForegroundColor Red
        exit 1
    }
}

# Verificar se já está rodando
$existing = Get-Process -Name "event_horizon" -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "eventhorizon ja esta rodando (PID: $($existing.Id))" -ForegroundColor Yellow
} else {
    # Iniciar eventhorizon
    Write-Host "Iniciando eventhorizon..." -ForegroundColor Yellow
    Start-Process -FilePath ".\build\windows-release\Release\event_horizon.exe" -WindowStyle Minimized
    Start-Sleep -Seconds 2
    
    # Verificar se iniciou
    $process = Get-Process -Name "event_horizon" -ErrorAction SilentlyContinue
    if ($process) {
        Write-Host "eventhorizon iniciado (PID: $($process.Id))" -ForegroundColor Green
    } else {
        Write-Host "ERRO: eventhorizon nao iniciou" -ForegroundColor Red
        exit 1
    }
}

# Subir Kafka UI
Write-Host ""
Write-Host "Subindo Kafka UI..." -ForegroundColor Yellow
docker-compose -f docker-compose.yml up -d

Write-Host ""
Write-Host "=========================================" -ForegroundColor Green
Write-Host "  eventhorizon Iniciado!" -ForegroundColor Green
Write-Host "=========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Servicos disponiveis:" -ForegroundColor Cyan
Write-Host "  - eventhorizon: localhost:9092" -ForegroundColor White
Write-Host "  - Kafka UI:   http://localhost:9080" -ForegroundColor White
Write-Host ""
Write-Host "Para parar: .\start-eventhorizon.ps1 -Stop" -ForegroundColor Gray
