# Node-RED Practice Workspace (`nodered-practice`)

Workspace **completamente separato** da `nodered-exam` e dai laboratori, pensato per esercitarsi con altri esercizi in vista dell'esame.

## Avvio Rapido

Da **IntelliJ IDEA** (Run Configuration **`Node-RED Practice (Exercises)`**) oppure da PowerShell nella cartella `nodered/`:

```powershell
.\start-nodered.ps1 -Mode practice
```

- Editor web Node-RED: **http://localhost:1881** *(porta dedicata, può girare in parallelo a `Node-RED Exam` su `1880`)*
- Broker MQTT Mosquitto (WSL2): **`localhost:1883`** (avviato automaticamente dallo script; preconfigurato anche `mqtt.neslab.it:3200`)
- File dei flow salvato in: [`flows.json`](./flows.json)
- Configurazione Node-RED (con `contextStorage` `memory` + `file` su disco): [`settings.js`](./settings.js)
- Cartella suggerita per esercizi con nodi `file` / `file in`: `nodered-practice/data/`

Per importare uno dei pattern dalle lezioni, premi **`Ctrl + I`** nell'editor e seleziona un file da `../nodered-lab-template/flows/`.
