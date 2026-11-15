# Ferma Mosquitto, cancella messaggi retained e sessioni persistenti (/var/lib/mosquitto/mosquitto.db) e riavvia da zero
Write-Host "Reset completo del broker MQTT Mosquitto (pulizia retained e sessioni persistenti)..." -ForegroundColor Magenta

& "$PSScriptRoot\stop-mqtt.ps1"

wsl -u root bash -c "rm -f /var/lib/mosquitto/mosquitto.db /var/log/mosquitto/mosquitto.log"

& "$PSScriptRoot\start-mqtt.ps1"
