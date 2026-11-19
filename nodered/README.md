# NSDS Node-RED

Questa cartella contiene l'ambiente Node-RED configurato, il broker MQTT locale, i flow template delle lezioni, gli appunti d'esame e il workspace pronto per i laboratori e l'esame della parte di **Node-RED** del corso **Networked Software for Distributed Systems (NSDS)** al **Politecnico di Milano**.

---

## 🛠️ Ambiente Pre-Installato (Windows + WSL2)

Tutto il necessario è già installato e configurato:
- **Node.js Runtime (Windows)**: `v24.22.1 LTS` (`%LOCALAPPDATA%\Programs\nodejs`)
- **Node-RED (Windows)**: `v5.0.9` (installato sia localmente in `nodered/node_modules` sia globalmente)
- **MQTT Broker — Eclipse Mosquitto (WSL2 Ubuntu)**: `mosquitto` + `mosquitto-clients` configurati in `/etc/mosquitto/conf.d/nsds.conf` per esporre **`localhost:1883`** verso Windows e Node-RED (`allow_anonymous true`).

---

## 📁 Struttura della Cartella

```text
nodered/
├── README.md                              # Questa guida
├── package.json                           # Dipendenze (node-red, mqtt) e npm scripts per l'IDE
├── start-mqtt.ps1                         # Avvia il broker Mosquitto in WSL2 (localhost:1883)
├── stop-mqtt.ps1                          # Ferma il broker Mosquitto in WSL2
├── reset-mqtt.ps1                         # Pulisce retained messages / sessioni persistenti e riavvia
├── start-nodered.ps1                      # Avvia Node-RED (exam o templates) + Mosquitto
├── stop-nodered.ps1                       # Ferma eventuali istanze Node-RED attive
├── lectures/                              # Slide ufficiali delle lezioni (PDF)
│   ├── 1.1-Node-RED Basics.pdf
│   ├── 1.2-Node-RED Processing Nodes.pdf
│   ├── 1.3-Node-RED Data Sharing.pdf
│   ├── 2.1-Node-RED Networking UDP.pdf
│   └── 2.2-Node-RED Networking MQTT.pdf
├── notes/
│   └── NodeRED_Lecture_Notes.md           # Appunti completi e cheatsheet d'esame
├── lab/                                   # Tracce e soluzioni dei laboratori (+ lab-userdir isolato)
├── nodered-lab-template/                  # Template pronti per tutte e 5 le lezioni
│   ├── README.md                          # Tabella decisionale e guida all'uso
│   ├── settings.js
│   ├── flows/
│   │   ├── 01_basics_and_async.json
│   │   ├── 02_function_nodes_patterns.json
│   │   ├── 03_data_sharing_files_and_context.json
│   │   ├── 04_udp_json_networking.json
│   │   ├── 05_mqtt_pubsub_and_qos.json
│   │   └── all_templates_combined.json
│   └── scripts/
│       ├── udp-test-client.js             # Client UDP di test (send / listen)
│       └── mqtt-test-client.js            # Client MQTT di test (pub / sub / sensor)
├── nodered-practice/                      # Workspace isolato per esercizi extra (:1881)
│   ├── README.md
│   ├── flows.json
│   ├── package.json                       # openweathermap + chatbot preinstallati
│   └── settings.js                        # Porta 1881 + contextStorage (memory + file)
└── nodered-exam/                          # Workspace pulito dedicato all'esame (:1880)
    ├── README.md
    ├── flows.json
    ├── package.json                       # openweathermap + chatbot preinstallati
    └── settings.js                        # Porta 1880 + contextStorage (memory + file)
```

---

## 🚀 Avvio, Arresto e Reset Rapido (da PowerShell)

Dalla cartella `nodered/` (o dalla root del repository):

### 1. Avviare Node-RED (con Mosquitto MQTT già pronto)
```powershell
# Avvia il workspace pulito per l'esame (http://localhost:1880)
.\nodered\start-nodered.ps1 -Mode exam

# Avvia il workspace separato per esercitarsi (http://localhost:1881 — anche in parallelo!)
.\nodered\start-nodered.ps1 -Mode practice

# Oppure avvia Node-RED con tutti i 5 tab dei template delle lezioni (http://localhost:1880)
.\nodered\start-nodered.ps1 -Mode templates
```
Modalità supportate per `-Mode`: `exam` (`1880`), `practice` (`1881`), `lab1`, `lab2`, `templates`, `basics`, `functions`, `datasharing`, `udp`, `mqtt`.
Per fermare Node-RED premi **`Ctrl+C`** nel terminale (oppure esegui `.\nodered\stop-nodered.ps1`).

### 2. Gestire solo il Broker MQTT Mosquitto
```powershell
.\nodered\start-mqtt.ps1   # Avvia Mosquitto su localhost:1883
.\nodered\stop-mqtt.ps1    # Ferma Mosquitto
.\nodered\reset-mqtt.ps1   # Cancella retained/sessioni persistenti e riavvia
```

---

## 💻 Uso nell'IDE (IntelliJ IDEA & VS Code)

### Da IntelliJ IDEA (consigliato — già configurato!)
1. **Run Configurations pronte in alto a destra**:
   - **`Node-RED Exam`** — Avvia Mosquitto + Node-RED sul workspace isolato `nodered-exam/` (`http://localhost:1880`).
   - **`Node-RED Practice (Exercises)`** — Avvia Mosquitto + Node-RED sul workspace isolato `nodered-practice/` (`http://localhost:1881`, utilizzabile anche contemporaneamente a `Node-RED Exam`).
   - **`Node-RED Lab 1`** / **`Node-RED Lab 2`** — Avvia i laboratori nella loro `userDir` separata (`lab/lab-userdir/`).
   - **`Node-RED Templates (All Lectures)`** — Avvia Mosquitto + Node-RED con tutti i 5 flow delle lezioni.
   - **`MQTT Start (WSL2)` / `MQTT Stop (WSL2)`** — Avvia o ferma solo il broker Mosquitto.
   - **`UDP Test Client (Send)` / `MQTT Test Client (Sensor)`** — Esegue gli script Node.js companion direttamente nella console dell'IDE.
2. **Dalla tool window `npm`** (oppure aprendo [`package.json`](./package.json) e cliccando sull'icona ▶ accanto a `"scripts"`):
   - `start:exam`, `start:practice`, `start:lab1`, `start:lab2`, `start:templates`, `start:basics`, `start:functions`, `start:datasharing`, `start:udp`, `start:mqtt`, `test:udp`, `test:mqtt`.
3. **Aprire l'editor visuale**: una volta avviato Node-RED, apri **`http://localhost:1880`** (Exam / Lab / Templates) oppure **`http://localhost:1881`** (Practice) nel browser (i file `flows.json` nel repo si aggiornano automaticamente ogni volta che premi **Deploy**).

> **Nota per IntelliJ IDEA appena aggiornato**: se il terminale integrato di IntelliJ era già aperto prima dell'installazione di Node.js, riavvia IntelliJ IDEA una volta affinché erediti il nuovo `PATH` di Windows (`node`, `npm`, `node-red`).

### Da VS Code
- Apri la root `nsds/`, usa la vista **NPM Scripts** nella sidebar di Explorer (su `nodered/package.json`) o il terminale integrato con `.\nodered\start-nodered.ps1`.

---

## 🔍 Comandi CLI Utili (MQTT Mosquitto)

Puoi ispezionare o testare i topic MQTT direttamente da PowerShell usando `mosquitto_sub` / `mosquitto_pub` in WSL2:

- **Ascoltare tutti i topic MQTT in tempo reale**:
  ```powershell
  wsl mosquitto_sub -h localhost -t '#' -v
  ```
- **Ascoltare un topic specifico (es. `/nodered/test` o `/smartcity/+`)**:
  ```powershell
  wsl mosquitto_sub -h localhost -t '/smartcity/+' -v
  ```
- **Pubblicare un messaggio JSON di test**:
  ```powershell
  wsl mosquitto_pub -h localhost -t '/nodered/test' -q 2 -m '{"hello":"from CLI"}'
  ```

---

## 📚 Appunti & Template Pronti

- **Appunti Completi & Playbook d'Esame**: [`notes/NodeRED_Lecture_Notes.md`](./notes/NodeRED_Lecture_Notes.md)
- **Indice dei Template & Script**: [`nodered-lab-template/README.md`](./nodered-lab-template/README.md)
- **Workspace d'Esame**: [`nodered-exam/README.md`](./nodered-exam/README.md)
