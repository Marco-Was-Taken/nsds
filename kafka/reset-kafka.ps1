# Ferma Kafka e ZooKeeper, cancella tutti i topic/offset (/tmp/kafka-logs e /tmp/zookeeper) e riavvia da zero
Write-Host "Reset completo di Kafka e ZooKeeper (pulizia topic, offset e transazioni)..." -ForegroundColor Magenta

& "$PSScriptRoot\stop-kafka.ps1"

wsl -u root bash -c "rm -rf /tmp/kafka-logs /tmp/zookeeper /tmp/kafka-server.log /tmp/zookeeper.log"

& "$PSScriptRoot\start-kafka.ps1"
