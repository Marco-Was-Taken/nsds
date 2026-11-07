# Ferma Kafka Broker e ZooKeeper in WSL2
Write-Host "Arresto di Kafka e ZooKeeper in WSL2..." -ForegroundColor Yellow

wsl -u root /opt/kafka/nsds-stop.sh

Write-Host "Kafka e ZooKeeper arrestati." -ForegroundColor Green
