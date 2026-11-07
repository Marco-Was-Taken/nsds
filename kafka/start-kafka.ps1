# Avvia ZooKeeper e Kafka Broker in WSL2 (/opt/kafka) in background
Write-Host "Avvio di ZooKeeper e Kafka in WSL2..." -ForegroundColor Cyan

wsl -u root /opt/kafka/nsds-start.sh

# Mantiene viva l'istanza WSL2 finche Kafka e in esecuzione
Start-Process wsl -ArgumentList "-u", "root", "bash", "-c", "while pgrep -f 'kafka\.Kafka' >/dev/null; do sleep 5; done" -WindowStyle Hidden

Write-Host "Kafka Broker attivo su localhost:9092!" -ForegroundColor Green
