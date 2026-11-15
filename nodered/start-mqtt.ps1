# Avvia il broker MQTT Mosquitto in WSL2 in background (esposto su localhost:1883)
Write-Host "Avvio del broker MQTT Mosquitto in WSL2..." -ForegroundColor Cyan

# Mantiene viva la distribuzione WSL2 Ubuntu in background
Invoke-WmiMethod -Class Win32_Process -Name Create -ArgumentList "wsl.exe -d Ubuntu --exec dbus-launch true" | Out-Null

wsl -u root /opt/mosquitto/nsds-start.sh
if ($LASTEXITCODE -ne 0) {
    Write-Host "Errore durante l'avvio di Mosquitto in WSL2." -ForegroundColor Red
    exit 1
}

Write-Host "Mosquitto MQTT Broker attivo su localhost:1883!" -ForegroundColor Green
