<#
.SYNOPSIS
    Executa benchmark de producao comparando Event Horizon vs Apache Kafka

.DESCRIPTION
    Este script usa o OpenMessaging Benchmark (ferramenta oficial da Confluent)
    para comparar performance de producao entre Event Horizon e Apache Kafka.
    
    Metricas coletadas:
    - Throughput (mensagens/segundo, MB/segundo)
    - Latencia (P50, P90, P95, P99, P99.9)

.PARAMETER Target
    Alvo do benchmark: "kafka", "eventhorizon" ou "compare" (ambos)

.PARAMETER Workload
    Tipo de workload: "throughput" ou "latency"

.PARAMETER MessageSize
    Tamanho da mensagem em bytes (default: 1024)

.PARAMETER MessageCount
    Quantidade de mensagens a enviar (default: 100000)

.PARAMETER Duration
    Duracao do teste em minutos (default: 2)

.EXAMPLE
    .\run-benchmark.ps1 -Target compare
    
.EXAMPLE
    .\run-benchmark.ps1 -Target eventhorizon -Workload latency

.NOTES
    Requisitos:
    - Docker Desktop rodando
    - Event Horizon rodando em localhost:9092 (se testando eventhorizon)
#>

param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("kafka", "eventhorizon", "compare")]
    [string]$Target = "compare",
    
    [Parameter(Mandatory=$false)]
    [ValidateSet("throughput", "latency")]
    [string]$Workload = "throughput",
    
    [Parameter(Mandatory=$false)]
    [int]$MessageSize = 1024,
    
    [Parameter(Mandatory=$false)]
    [int]$MessageCount = 100000,
    
    [Parameter(Mandatory=$false)]
    [int]$Duration = 2
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host ""
Write-Host "================================================================" -ForegroundColor Blue
Write-Host "  Benchmark de Producao: Event Horizon vs Apache Kafka" -ForegroundColor Blue  
Write-Host "================================================================" -ForegroundColor Blue
Write-Host ""
Write-Host "Configuracao:" -ForegroundColor Cyan
Write-Host "  Target:        $Target"
Write-Host "  Workload:      $Workload"
Write-Host "  Message Size:  $MessageSize bytes"
Write-Host "  Message Count: $MessageCount"
Write-Host "  Duration:      $Duration minutos"
Write-Host ""

# Criar diretorio de resultados
$ResultsDir = Join-Path $ScriptDir "results"
if (-not (Test-Path $ResultsDir)) {
    New-Item -ItemType Directory -Path $ResultsDir | Out-Null
}

# Verificar se Event Horizon esta rodando (se necessario)
if ($Target -eq "eventhorizon" -or $Target -eq "compare") {
    Write-Host "Verificando Event Horizon em localhost:9092..." -ForegroundColor Yellow
    try {
        $tcpClient = New-Object System.Net.Sockets.TcpClient
        $tcpClient.Connect("localhost", 9092)
        $tcpClient.Close()
        Write-Host "[OK] Event Horizon esta rodando" -ForegroundColor Green
    }
    catch {
        Write-Host "[ERRO] Event Horizon nao esta rodando em localhost:9092" -ForegroundColor Red
        Write-Host ""
        Write-Host "Inicie o Event Horizon primeiro:" -ForegroundColor Yellow
        Write-Host "  cd .." -ForegroundColor Gray
        Write-Host "  .\build\windows-debug\Debug\event_horizon.exe" -ForegroundColor Gray
        Write-Host ""
        exit 1
    }
}

# Mover para diretorio do script
Push-Location $ScriptDir

try {
    $WorkloadFile = "$Workload-1kb.yaml"
    
    # Definir variaveis de ambiente para docker-compose
    $env:NUM_RECORDS = "$MessageCount"
    $env:RECORD_SIZE = "$MessageSize"
    
    switch ($Target) {
        "kafka" {
            Write-Host ""
            Write-Host "--- Executando benchmark contra Apache Kafka ---" -ForegroundColor Cyan
            Write-Host ""
            
            docker-compose -f docker-compose-benchmark.yml up -d kafka
            Write-Host "Aguardando Kafka iniciar..." -ForegroundColor Yellow
            Start-Sleep -Seconds 20
            docker-compose -f docker-compose-benchmark.yml run --rm benchmark-kafka
            docker-compose -f docker-compose-benchmark.yml down
        }
        
        "eventhorizon" {
            Write-Host ""
            Write-Host "--- Executando benchmark contra Event Horizon ---" -ForegroundColor Cyan
            Write-Host ""
            
            docker-compose -f docker-compose-benchmark.yml run --rm benchmark-eventhorizon
        }
        
        "compare" {
            Write-Host ""
            Write-Host "--- Executando benchmark contra Apache Kafka ---" -ForegroundColor Cyan
            Write-Host ""
            
            docker-compose -f docker-compose-benchmark.yml up -d kafka
            Write-Host "Aguardando Kafka iniciar..." -ForegroundColor Yellow
            Start-Sleep -Seconds 20
            docker-compose -f docker-compose-benchmark.yml run --rm benchmark-kafka
            
            Write-Host ""
            Write-Host "--- Executando benchmark contra Event Horizon ---" -ForegroundColor Cyan
            Write-Host ""
            
            docker-compose -f docker-compose-benchmark.yml run --rm benchmark-eventhorizon
            
            docker-compose -f docker-compose-benchmark.yml down
            
            Write-Host ""
            Write-Host "--- Comparacao de Resultados ---" -ForegroundColor Magenta
            Write-Host ""
            
            if (Test-Path "$ResultsDir\kafka-producer-benchmark.txt") {
                Write-Host "KAFKA:" -ForegroundColor Yellow
                Get-Content "$ResultsDir\kafka-producer-benchmark.txt" | Select-Object -Last 5
                Write-Host ""
            }
            
            if (Test-Path "$ResultsDir\eventhorizon-producer-benchmark.txt") {
                Write-Host "EVENT HORIZON:" -ForegroundColor Yellow
                Get-Content "$ResultsDir\eventhorizon-producer-benchmark.txt" | Select-Object -Last 5
                Write-Host ""
            }
            
            Write-Host "Resultados completos em: $ResultsDir" -ForegroundColor Green
        }
    }
}
finally {
    Pop-Location
}

Write-Host ""
Write-Host "Benchmark concluido!" -ForegroundColor Green
Write-Host ""
