# Node-RED Lab & Exam Templates — Indice Rapido

Questa cartella raccoglie i **flow template pronti all'uso** (file `.json` importabili in Node-RED o avviabili direttamente) e gli **script Node.js di test** per coprire tutti gli argomenti delle 5 lezioni.

> **Appunti completi da esame**: consulta [`NodeRED_Lecture_Notes.md`](../notes/NodeRED_Lecture_Notes.md).

---

## 🧭 Quale Flow Template Usare? (Tabella Decisionale)

| Lezione & Argomento | File Flow (`flows/`) | Contenuto / Pattern |
| :--- | :--- | :--- |
| **1.1 Basics & Async** | [`01_basics_and_async.json`](./flows/01_basics_and_async.json) | `inject` timestamp, `switch` routing (`Temperature > 15C?`), dimostrazione esecuzione **asincrona non-deterministica** su 3 rami paralleli |
| **1.2 Processing / Function Nodes** | [`02_function_nodes_patterns.json`](./flows/02_function_nodes_patterns.json) | Hook `On Start` / `On Message` / `On Stop`, parity check, **override** e **slicing** del payload, **multi-output** (`[msg, null, null]`), **multi-message** (`return [msgList]`) |
| **1.3 Data Sharing & State** | [`03_data_sharing_files_and_context.json`](./flows/03_data_sharing_files_and_context.json) | Contatore su **File** (`file` / `file in` + boot `once`), **Node Context** (`context.get/set`), **Flow & Global Context**, store persistente `"file"` (`localfilesystem`) |
| **2.1 Networking UDP** | [`04_udp_json_networking.json`](./flows/04_udp_json_networking.json) | `udp out` / `udp in` su `127.0.0.1:7777` con conversione esplicita `json` (`JS Object <-> JSON String`) |
| **2.2 Networking MQTT** | [`05_mqtt_pubsub_and_qos.json`](./flows/05_mqtt_pubsub_and_qos.json) | Pub/Sub su `localhost:1883` (`/nodered/test`, QoS 2), topic gerarchici & wildcard (`/smartcity/+`), auto-JSON e parser **Sensor.Community** |
| **Tutti insieme (Consigliato)** | [`all_templates_combined.json`](./flows/all_templates_combined.json) | Unisce tutti e 5 i tab in un'unica schermata Node-RED! |

---

## ▶️ Come Avviare o Importare i Template

### Opzione A — Avvio diretto di tutti i template (consigliato)
Dalla cartella `nodered/`:
```powershell
.\start-nodered.ps1 -Mode templates
```
*(Avvia automaticamente anche il broker Mosquitto su `localhost:1883` e apre tutti e 5 i tab su `http://localhost:1880`.)*

Puoi anche avviare una singola lezione:
```powershell
.\start-nodered.ps1 -Mode basics       # 01_basics_and_async.json
.\start-nodered.ps1 -Mode functions    # 02_function_nodes_patterns.json
.\start-nodered.ps1 -Mode datasharing  # 03_data_sharing_files_and_context.json
.\start-nodered.ps1 -Mode udp          # 04_udp_json_networking.json
.\start-nodered.ps1 -Mode mqtt         # 05_mqtt_pubsub_and_qos.json
```

### Opzione B — Importare un singolo template nel tuo flow d'esame (`nodered-exam`)
1. Apri Node-RED su `http://localhost:1880`.
2. Premi **`Ctrl + I`** (oppure Menu `≡` $\to$ **Import**).
3. Clicca **select a file to import** e scegli uno dei file in `nodered-lab-template/flows/`.

---

## 🧪 Script Node.js Companion (`scripts/`)

Utili per testare la comunicazione tra **Node-RED** e **applicazioni esterne** (come descritto nella Lezione 2.1):

```powershell
# Dalla cartella nodered/

# Invia un pacchetto UDP JSON verso Node-RED (porta 7777)
node .\nodered-lab-template\scripts\udp-test-client.js send 7777

# Ascolta pacchetti UDP JSON inviati da Node-RED
node .\nodered-lab-template\scripts\udp-test-client.js listen 7777

# Pubblica su MQTT /nodered/test (richiede .\start-mqtt.ps1)
node .\nodered-lab-template\scripts\mqtt-test-client.js pub /nodered/test

# Pubblica un campione Sensor.Community su /smartcity/milan
node .\nodered-lab-template\scripts\mqtt-test-client.js sensor

# Sottoscriviti da console a qualsiasi topic (es. '#' o '/smartcity/+')
node .\nodered-lab-template\scripts\mqtt-test-client.js sub '#'
```
