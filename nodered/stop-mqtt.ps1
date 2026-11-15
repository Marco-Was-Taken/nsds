# Ferma il broker MQTT Mosquitto in WSL2
Write-Host "Arresto di Mosquitto MQTT Broker in WSL2..." -ForegroundColor Yellow

wsl -u root /opt/mosquitto/nsds-stop.sh

Write-Host "Mosquitto MQTT Broker arrestato." -ForegroundColor Green
