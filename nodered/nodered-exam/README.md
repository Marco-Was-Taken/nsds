# Node-RED Exam Workspace (`nodered-exam`)

Modulo pulito e isolato pronto esclusivamente per l'esame di **Node-RED**.
*(Per i laboratori usa `Node-RED Lab 1` / `Node-RED Lab 2`; per esercitarti con altri esercizi usa `Node-RED Practice (Exercises)` su `http://localhost:1881`.)*

## Avvio Rapido

Da **IntelliJ IDEA** (Run Configuration **`Node-RED Exam`**) oppure da PowerShell nella cartella `nodered/`:

```powershell
.\start-nodered.ps1 -Mode exam
```

- Editor web Node-RED: **http://localhost:1880**
- Broker MQTT Mosquitto (WSL2): **`localhost:1883`** (avviato automaticamente dallo script)
- File dei flow salvato automaticamente in: [`flows.json`](./flows.json)
- Configurazione Node-RED (con `contextStorage` `memory` + `file` su disco): [`settings.js`](./settings.js)

Per importare uno dei pattern dalle lezioni, premi **`Ctrl + I`** nell'editor e seleziona un file da `../nodered-lab-template/flows/`.
