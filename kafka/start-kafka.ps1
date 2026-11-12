# Avvia ZooKeeper e Kafka Broker in WSL2 (/opt/kafka) in background
Write-Host "Avvio di ZooKeeper e Kafka in WSL2..." -ForegroundColor Cyan

# Mantiene viva la distribuzione WSL2 Ubuntu in background
Invoke-WmiMethod -Class Win32_Process -Name Create -ArgumentList "wsl.exe -d Ubuntu --exec dbus-launch true" | Out-Null

wsl -u root /opt/kafka/nsds-start.sh
if ($LASTEXITCODE -ne 0) {
    Write-Host "Errore durante l'avvio di Kafka in WSL2." -ForegroundColor Red
    exit 1
}

Write-Host "Kafka Broker attivo su localhost:9092!" -ForegroundColor Green


