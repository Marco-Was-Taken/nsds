# Node-RED Lab 2

Workspace pronto per il **Lab 2 di Node-RED** (`http://localhost:1880`).

## Avvio Rapido

- **Da IntelliJ IDEA**: seleziona la Run Configuration **`Node-RED Lab 2`** e premi Run.
- **Da PowerShell** (dalla cartella `nodered/`):
  ```powershell
  .\start-nodered.ps1 -Mode lab2
  ```
- **Da npm** (dalla cartella `nodered/`):
  ```powershell
  npm run start:lab2
  ```

## Risorse Preconfigurate

- **Flow (`flows.json`)**: [`flows.json`](./flows.json) contiene 3 tab pronti:
  1. **`Lab 2 - Example`**: schema generico funzionante e testabile con un clic:
     - **UDP + JSON + Flow Context** (`127.0.0.1:7777` $\to$ `flow.lastUdpTelemetry`)
     - **MQTT Pub/Sub (`localhost:1883`, QoS 1) + Persistent `file` Context** (`nsds/lab2/events` $\to$ `mqttCount` su disco + `flow.lastMqttEvent`)
     - **Chatbot (`node-red-contrib-chatbot`)** con `chatbot-rules` (`/status` vs `catchAll`) e `chatbot-message` che legge lo stato di `flow`
  2. **`Lab 2 - Telegram Template`** *(disabilitato di default)*: flusso completo `chatbot-telegram-receive` $\to$ `chatbot-rules` $\to$ `chatbot-message` $\to$ `chatbot-telegram-send` (inserisci il token nel nodo `Lab2TelegramBot` e abilita il tab per usarlo)
  3. **`Lab 2 - Exercises`**: tab pulito per svolgere gli esercizi del laboratorio
- **Broker MQTT già definiti nei config nodes**:
  - `localhost:1883` (Mosquitto locale WSL2, avviato automaticamente da `start-nodered.ps1 -Mode lab2`, `autoConnect: true`)
  - `mqtt.neslab.it:3200` (broker NESLab, `autoConnect: false` — abilitalo/selezionalo se richiesto dalla traccia)
- **Cartella dati locale**: `./lab/lab2/data/` (per eventuali nodi `file` / `file in`)
- **Context Storage**: `memory` (`default`) e `localfilesystem` (`"file"`) isolati in `../lab-userdir/settings.js` (separati da `nodered-exam`)
- **Template importabili (`Ctrl + I`)**: `../../nodered-lab-template/flows/` (`04_udp_json_networking.json`, `05_mqtt_pubsub_and_qos.json`, ecc.)

## Script di Test Utili (UDP / MQTT)

```powershell
# Dalla cartella nodered/
node .\nodered-lab-template\scripts\udp-test-client.js send 7777
node .\nodered-lab-template\scripts\udp-test-client.js listen 7777
node .\nodered-lab-template\scripts\mqtt-test-client.js sub '#'
node .\nodered-lab-template\scripts\mqtt-test-client.js pub /nodered/test
```
