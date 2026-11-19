# Node-RED — Fast Exam Reference & Code Playbook (NSDS)

Appunti completi e prontuario operativo basati sulle lezioni, sul **Lab 1** e sul **Lab 2**:
- [`1.1-Node-RED Basics.pdf`](../lectures/1.1-Node-RED%20Basics.pdf)
- [`1.2-Node-RED Processing Nodes.pdf`](../lectures/1.2-Node-RED%20Processing%20Nodes.pdf)
- [`1.3-Node-RED Data Sharing.pdf`](../lectures/1.3-Node-RED%20Data%20Sharing.pdf)
- [`2.1-Node-RED Networking UDP.pdf`](../lectures/2.1-Node-RED%20Networking%20UDP.pdf)
- [`2.2-Node-RED Networking MQTT.pdf`](../lectures/2.2-Node-RED%20Networking%20MQTT.pdf)
- [`Node-RED Lab 1.pdf`](../lab/lab1/Node-RED%20Lab%201.pdf) & [`lab/lab1/flows.json`](../lab/lab1/flows.json)
- [`Node-RED Lab 2.pdf`](../lab/lab2/Node-RED%20Lab%202.pdf), [`lab/lab2/exercise8-0.json`](../lab/lab2/exercise8-0.json) & [`lab/lab2/flows.json`](../lab/lab2/flows.json)

---

## 1. Matrice Rapida: Quale Pattern Usare?

| Esigenza / Scenario d'Esame | Meccanismo Node-RED | Riferimento / Template |
| :--- | :--- | :--- |
| Generare un evento manuale, periodico o **solo all'avvio** | `inject` (`repeat`, oppure `once: true` per boot) | [`01_basics_and_async.json`](../nodered-lab-template/flows/01_basics_and_async.json) |
| Instradare messaggi su rami diversi senza codice JS | `switch` (es. `payload > 15` vs `otherwise`) + `change` | [`01_basics_and_async.json`](../nodered-lab-template/flows/01_basics_and_async.json) |
| Trasformare, filtrare o elaborare `msg.payload` in JS | `function` (`On Start`, `On Message`, `On Stop`) | [`02_function_nodes_patterns.json`](../nodered-lab-template/flows/02_function_nodes_patterns.json) |
| Inviare messaggi su **più uscite distinte** da un `function` | Impostare `Outputs = N` e fare `return [msg, null, null]` | [`02_function_nodes_patterns.json`](../nodered-lab-template/flows/02_function_nodes_patterns.json) |
| Emettere **una sequenza di $K$ messaggi** su un'uscita | `return [msgList];` (array di array!) oppure `node.send([msgList]);` | [`02_function_nodes_patterns.json`](../nodered-lab-template/flows/02_function_nodes_patterns.json) |
| Condividere stato tra invocazioni **dello stesso nodo** | `context.get("k")` / `context.set("k", v)` | [`03_data_sharing_files_and_context.json`](../nodered-lab-template/flows/03_data_sharing_files_and_context.json) |
| Condividere stato tra catene dello **stesso tab/flow** | `flow.get("k")` / `flow.set("k", v)` | [`03_data_sharing_files_and_context.json`](../nodered-lab-template/flows/03_data_sharing_files_and_context.json) |
| Condividere stato tra **tab diversi** della stessa istanza | `global.get("k")` / `global.set("k", v)` | [Sezione 7.3](#73-flow-vs-global-tra-catene-dello-stesso-tab-e-tab-separati) |
| Mantenere lo stato tra **deploy e riavvii** | Nodi `file` / `file in` **oppure** `contextStorage` su `localfilesystem` (`context.get("k", "file")`) | [`03_data_sharing_files_and_context.json`](../nodered-lab-template/flows/03_data_sharing_files_and_context.json) |
| Chiamare un'**API REST / HTTP GET** e ottenere un oggetto JS | `http request` (`Method = GET`, `Return = a parsed JSON object`) | [Sezione 7.1](#71-nodo-http-request-http-get--rest-api) |
| Leggere meteo corrente da **OpenWeatherMap** (fisso o dinamico) | `openweathermap` (`city`/`country` fissi, oppure vuoti + **`msg.location = { city, country }`**) $\to$ `msg.payload.tempc` | [Sezione 7.2](#72-nodo-openweathermap-node-red-node-openweathermap) |
| Salvare/leggere **log JSON** su file (singolo vs storico NDJSON) | `overwrite` $\to$ `file in` (`utf8`) $\to$ `json`; `append` $\to$ `file in` (**`one msg per line`**) $\to$ `json` | [Sezione 7.4](#74-log-json-su-file-overwrite-vs-append-ndjson) |
| Inviare/ricevere oggetti JS via **UDP** (`127.0.0.1:port`) | `json` (`Object -> JSON string`) $\to$ `udp out` / `udp in` $\to$ `json` (`JSON string -> Object`) | [`04_udp_json_networking.json`](../nodered-lab-template/flows/04_udp_json_networking.json) |
| Pub/Sub disaccoppiato su topic gerarchici (**MQTT**) | `mqtt out` / `mqtt in` verso `localhost:1883` o `mqtt.neslab.it:3200` (`QoS 0/1/2`, auto-JSON) | [`05_mqtt_pubsub_and_qos.json`](../nodered-lab-template/flows/05_mqtt_pubsub_and_qos.json) |
| Costruire un **Bot Telegram** con risposte dinamiche | `chatbot-telegram-receive` $\to$ `chatbot-rules` / `function` $\to$ `chatbot-message` (`Text`) $\to$ `chatbot-telegram-send` | [Sezione 9 (Chatbot & Telegram)](#9-nodi-e-pattern-dal-lab-2--chatbot--telegram-node-red-contrib-chatbot) |
| Inserire dati di flusso (es. meteo) nella risposta del bot | Collegare `openweathermap` $\to$ `Text` (`chatbot-message`) con `{{payload.tempc}}` / `{{payload.message}}` | [Sezione 9.3](#93-nodo-text-chatbot-message--sintassi-handlebars) |
| Gestire **stato multi-utente** + **fan-out di $K$ query** da un `msg` | Mappa `context.get("userPrefs")` indicizzata per `String(from.id)` + `RED.util.cloneMessage(msg)` in ciclo + `return [msgList, null]` | [Sezione 14 (Weather Tracker Bot)](#14-case-study-desame-2--telegram-weather-tracker-bot-query--track--report) |
| Rivedere tutte le soluzioni e i codici del **Lab 1** (Ex 0–6) | Tabella + pattern completi pronti all'uso | [Sezione 8 (Lab 1 Playbook)](#8-guida-completa-agli-esercizi-del-lab-1-soluzioni-codice-e-trappole) |
| Rivedere tutte le soluzioni e i codici del **Lab 2** (Ex 5-0 – 5-2) | Telegram Bot, meteo dinamico e log persistente utenti | [Sezione 10 (Lab 2 Playbook)](#10-guida-completa-agli-esercizi-del-lab-2-soluzioni-codice-e-trappole) |
| Case Study d'Esame 1 (**Telegram Weather & Wind Forecast Bot**) | Tokenizzazione sicura, `forecast` multi-giorno, change detection, log ogni minuto | [Sezione 12 (Case Study 1)](#12-case-study-desame--telegram-weather--wind-forecast-bot-evaluation-lab) |
| Case Study d'Esame 2 (**Telegram Weather Tracker Bot**) | `QUERY` / `TRACK` / `REPORT`, OpenWeather dinamico, stato multi-utente, fan-out `RED.util.cloneMessage` | [Sezione 14 (Case Study 2)](#14-case-study-desame-2--telegram-weather-tracker-bot-query--track--report) |
| Fare **parsing / tokenizzazione** di stringhe e **Regex** in JS | `.trim()`, `.split(/\s+/)`, `.match(/[a-z0-9]+/g) \|\| []`, `Number()`, `JSON.parse()` | [Sezione 13 (Parsing & Regex JS)](#13-riferimento-rapido-js--parsing-di-stringhe-tokenizzazione--regex) |

---

## 2. Fondamenti & Modello di Esecuzione (Lezione 1.1)

### 2.1 Cos'è Node-RED
- Tool visuale basato su **flow-based programming** (dataflow programming), costruito sopra **Node.js** (event-driven, non-blocking, single-threaded event loop).
- Somiglianze concettuali:
  - **Dataflow / Petri-nets**: i dati (*token*) fluiscono tra operatori (*place/node*) lungo gli archi diretti (*wires*).
  - **Actor model**: i nodi si comportano come attori reattivi che reagiscono all'arrivo di **messaggi**.
- I flow vengono salvati, esportati e importati come file **JSON** (`flows.json`).
- **Deploy**: finché non premi **Deploy** (in alto a destra), le modifiche fatte nell'editor **non** sono attive sul runtime locale.

### 2.2 Struttura del Messaggio (`msg`)
Ogni messaggio è un **oggetto JavaScript** con tre proprietà fondamentali (più eventuali proprietà di protocollo):
- **`msg.topic`** (`string`): descrive il canale logico/categoria del messaggio.
- **`msg.payload`** (`any`): il contenuto effettivo (numero, stringa, booleano, array, oggetto JS, `Buffer`).
- **`msg._msgid`** (`string`): identificatore univoco assegnato all'interno dell'istanza Node-RED in esecuzione (nelle slide viene spesso chiamato `messageId`, in codice JS è **`msg._msgid`**).
- Altri campi comuni in networking: `msg.qos`, `msg.retain` (MQTT), `msg.ip`, `msg.port` (UDP).

### 2.3 Esecuzione Asincrona e Non-Deterministica
- L'esecuzione dei nodi è **asincrona** e schedulata in modo **non-deterministico** dall'event loop di Node.js.
- Se un nodo (es. `inject`) ha **più fili uscenti** verso rami paralleli (`"Hello" + 1`, `"Hello" + 2`, `"Hello" + 3`):
  - Ogni ramo riceve una **copia/messaggio indipendente**.
  - **Qualsiasi interleaving** dei messaggi in uscita sulla console di debug è possibile e può cambiare da un'esecuzione all'altra!
  - **Regola pratica**: non assumere mai l'ordine di arrivo tra rami paralleli; se serve ordinamento, metti i nodi **in serie** sullo stesso ramo.

---

## 3. Function Nodes & Pattern JavaScript (Lezione 1.2)

### 3.1 I 3 Tab del Nodo `function`
1. **On Start** (`initialize`): eseguito **una sola volta** quando il flow viene deployato/avviato (utile per inizializzare context o status).
2. **On Message** (`func`): eseguito **ogni volta** che arriva un messaggio `msg`.
3. **On Stop** (`finalize`): eseguito quando il flow si ferma o prima di un re-deploy (utile per cleanup).

### 3.2 Pattern Principali

#### A) Modificare o filtrare `msg`
```js
if (msg.payload % 2 === 0) {
    msg.payload = "Time is even";
} else {
    msg.payload = "Time is odd";
}
return msg;
// Per bloccare/scartare un messaggio (nessun output):
// return null;
```

#### B) Sovrascrivere (`Override`) o Estrarre (`Slicing`) il Payload
```js
// Override completo creando un nuovo oggetto messaggio
let newMsg = {
    topic: msg.topic,
    payload: "New message!",
    _msgid: msg._msgid
};
return newMsg;

// Slicing di un campo da un payload oggetto (es. { temperature: 22.1, humidity: 60 })
let slicedMsg = {
    topic: msg.topic,
    payload: msg.payload.temperature,
    _msgid: msg._msgid
};
return slicedMsg;
```

#### C) Multi-Output Function Node (`Outputs = N`)
Configura il numero di uscite (**Setup $\to$ Outputs = 3**) e restituisci un **array di lunghezza esattamente $N$**. Gli elementi `null` **non** generano messaggi sull'uscita corrispondente:
```js
if (msg.payload === "high") {
    return [msg, null, null]; // esce solo sulla porta 1
} else if (msg.payload === "med") {
    return [null, msg, null]; // esce solo sulla porta 2
} else {
    return [null, null, msg]; // esce solo sulla porta 3
}
```

#### D) Multi-Message Function Node (Emettere una sequenza di messaggi)
> ⚠️ **Trappola classica di Node-RED**: sulle slide viene mostrato `return msgList;` (array piatto 1D). In Node-RED, però, un array 1D restituito da `return` viene interpretato come **un elemento per porta di uscita** (quindi con 1 uscita invierebbe solo `msgList[0]`!).
> Per inviare **tutti** gli elementi di `msgList` in sequenza su una singola uscita, devi restituire un **array annidato** `[msgList]`:

```js
let msgList = [];
for (let i = 0; i < 10; i++) {
    msgList.push({ payload: i });
}
return [msgList]; // array di array -> tutti i 10 messaggi escono in sequenza sulla porta 1!
```
*(In alternativa asincrona, puoi chiamare `node.send([msgList]);` e poi `return null;`.)*

**Fan-out clonando un messaggio in ingresso (es. RedBot / Telegram `REPORT` su $K$ città):**
Quando devi generare $K$ messaggi derivati dallo **stesso `msg` in ingresso** (preservando `msg.originalMessage`, `msg.chat()`, ecc. per rispondere allo stesso utente):
```js
let msgList = [];
for (const item of items) {
    // Crea una copia profonda DENTRO il ciclo (preserva anche msg.chat() di RedBot)
    let cloned = RED.util.cloneMessage(msg);
    cloned.location = { city: item.city, country: item.country };
    msgList.push(cloned);
}
// Su un nodo con 2 uscite: msgList esce tutto sulla porta 1, nulla sulla porta 2
return [msgList, null];
```
> ⚠️ **3 errori letali da evitare**:
> 1. Dichiarare `let cloned = {}` **fuori dal `for`** $\to$ sovrascrivi sempre lo stesso oggetto e `msgList` finisce con $K$ copie identiche dell'ultimo elemento.
> 2. Usare `Object.assign({}, msg)` invece di `RED.util.cloneMessage(msg)` $\to$ fa una copia *shallow* (tutti i messaggi condividono lo stesso `msg.payload` e `_msgid`).
> 3. Restituire `[msg, null]` o `[items, null]` (i dati grezzi) invece di `[msgList, null]` $\to$ nel primo caso esce solo l'ultimo elemento, nel secondo perdi `originalMessage`/`chat()` e RedBot non può rispondere.

---

## 4. Data Sharing & Persistenza dello Stato (Lezione 1.3)

Quando lo stato deve sopravvivere tra più invocazioni o essere condiviso tra nodi/flow diversi, hai due strumenti principali:

### 4.1 Condivisione tramite File (`file` & `file in`)
- **Scrittura (`file` out)**:
  - `Overwrite file` (`true`) per mantenere un singolo valore aggiornato (es. contatore), oppure `Append` (`false`) per log.
  - Per inizializzare il file all'avvio: usa un nodo `inject` con **"Inject once after 0.1 seconds"** (`once: true`) che scrive `"0"` sul nodo `file`.
- **Lettura (`file in`)**:
  - Restituisce il contenuto come **stringa** (`utf8`) o `Buffer`.
  - Ricorda di convertire con `parseInt(msg.payload, 10)` o `parseFloat(msg.payload)` (o nodo `json` se salvi JSON!).

### 4.2 Context Objects (`context`, `flow`, `global`)
| Oggetto | Visibilità | API |
| :--- | :--- | :--- |
| **`context`** | Solo il **singolo nodo** `function` | `context.get("key")`, `context.set("key", val)` |
| **`flow`** | Tutti i nodi dello **stesso tab (flow)** | `flow.get("key")`, `flow.set("key", val)` |
| **`global`** | Tutti i nodi di **tutti i tab** della stessa istanza Node-RED | `global.get("key")`, `global.set("key", val)` |

#### Trappole da evitare con Context:
1. **Controllo valore iniziale `0`**:
   - Scrivere `if (!context.get("value"))` funziona solo se `value` non torna mai a `0`, perché `!0 === true` in JavaScript!
   - Forma sicura:
     ```js
     let value = context.get("value");
     if (value === undefined) {
         value = 0;
     }
     msg.payload = value;
     context.set("value", value + 1);
     return msg;
     ```
2. **Volatilità su Deploy / Riavvio (e portabilità all'esame)**:
   - Con lo store di default (`memory`), il context viene **azzerato al riavvio di Node-RED** (e anche su *Full Deploy*; con *Modified Nodes* si resetta solo per i nodi modificati).
   - Per renderlo **persistente su disco**, in `settings.js` si configura `contextStorage` con `"localfilesystem"` (già configurato in `nodered-exam/settings.js` e `nodered-lab-template/settings.js`!):
     ```js
     // Legge/scrive sullo store persistente chiamato "file"
     const count = context.get("count", "file") || 0;
     context.set("count", count + 1, "file");
     ```
   - ⚠️ **Attenzione per la consegna d'esame**: non passare `"file"` come secondo argomento nel flow che consegni se non è esplicitamente richiesto — sul Node-RED del docente lo store `"file"` potrebbe non esistere in `settings.js` (`Unknown context store 'file'`). Se serve persistenza portabile ovunque, usa i nodi `file` / `file in`.
3. **Chiavi dinamiche o numeriche in `context.get(key)` (Multi-Utente)**:
   - Node-RED interpreta `key` come *property expression*. Se passi un **numero puro** (es. `chatId = 12345`) a `context.set(12345, val)`, Node-RED lo salva sotto la chiave **`"undefined"`** sovrascrivendo tutti gli utenti! Anche un `chatId` negativo (chat di gruppo) può confondere il parser.
   - **Regola d'oro**: salva sempre un'unica **mappa sotto una chiave stringa fissa** (`"userPrefs"`) indicizzata per `String(userId)`:
     ```js
     const allPrefs = context.get("userPrefs") || {};
     const userId = String(msg.originalMessage?.from?.id ?? msg.payload?.userId ?? msg.payload?.chatId);
     const userPrefs = allPrefs[userId] || [];
     // ... modifica userPrefs ...
     allPrefs[userId] = userPrefs;       // obbligatorio la prima volta che l'utente è nuovo!
     context.set("userPrefs", allPrefs); // salva la mappa aggiornata nel context
     ```

---

## 5. Networking UDP & Serializzazione JSON (Lezione 2.1)

### 5.1 Perché serve la Serializzazione
- In memoria `msg.payload` può essere un oggetto JS complesso, un array, un numero...
- La rete è un canale di **byte seriali**: se invii un oggetto JS grezzo su un socket UDP senza serializzarlo, otterrai `[object Object]` o un formato inutilizzabile dal ricevente.
- **Regola**: converti esplicitamente da **JS Object $\to$ JSON String** prima di `udp out`, e da **JSON String $\to$ JS Object** dopo `udp in`.

### 5.2 Nodi UDP (`udp in` & `udp out`)
- **UDP**: protocollo leggero, **connectionless**, **best-effort** (nessuna garanzia di consegna né di ordine).
- **`udp out`**:
  - Specifica `Address` (es. `127.0.0.1` per test locali) e `Port` (es. `7777`).
  - Il contenuto inviato viene preso da `msg.payload`.
  - **Modalità dinamica**: se lasci *Address* e *Port* vuoti nel nodo `udp out`, Node-RED legge la destinazione da **`msg.ip`** e **`msg.port`**.
- **`udp in`**:
  - Specifica la porta su cui ascoltare (es. `7777`) e il formato di output (`a Buffer`, `a utf8 String`, `a Base64 encoded string`).
  - Se ricevi JSON, imposta Output = `a utf8 String` e collegalo a un nodo `json`.
- **Nodo `json`**:
  - **Evita `Always auto-detect` se vuoi sicurezza**: le slide segnalano esplicitamente che l'auto-detect può sbagliare. Configura manualmente:
    - Prima di `udp out`: `Action = Always convert to JSON String` (`"str"`).
    - Dopo `udp in`: `Action = Always convert to JavaScript Object` (`"obj"`).

---

## 6. Networking MQTT (Lezione 2.2)

### 6.1 Modello Publish / Subscribe
- **Disaccoppiamento**: Publisher e Subscriber non devono conoscersi né conoscere IP/porte reciproci; comunicano solo tramite un **Message Broker** centrale (qui **Mosquitto** su `localhost:1883`).
- Comunicazione **data-centric** (basata su topic) invece che address-centric (come UDP).
- **Topic gerarchici**: separati da `/` (es. `iot`, `iot/building21`, `iot/building21/temperature`, `/smartcity/milan`).
  - Wildcard nelle subscription (`mqtt in`):
    - `+`: singolo livello (es. `iot/+/temperature` matcha `iot/building21/temperature` ma non `iot/a/b/temperature`).
    - `#`: multi-livello finale (es. `iot/#` matcha tutto sotto `iot/`).

### 6.2 Nodi MQTT in Node-RED (`mqtt in` & `mqtt out`)
- **Broker Config Node**: `localhost`, porta `1883`.
- **Serializzazione automatica**:
  - A differenza di UDP, in MQTT i messaggi viaggiano come stringhe/buffer e **`mqtt out` converte automaticamente gli oggetti JS in JSON** (non serve un nodo `json` prima di `mqtt out`).
  - In ingresso su **`mqtt in`**, puoi impostare `Output = a parsed JSON object` (oppure `auto-detect`) per ricevere direttamente un oggetto JS in `msg.payload`.
- **Topic dinamico**: se lasci vuoto il campo `Topic` nel nodo `mqtt out`, verrà usato **`msg.topic`**.

### 6.3 Garanzie di Consegna: MQTT QoS (`0`, `1`, `2`)
| Livello QoS | Garanzia di Consegna | Protocollo / Handshake | Note |
| :--- | :--- | :--- | :--- |
| **QoS 0** | **At-Most-Once** *(0 o 1 volta)* | `PUBLISH` (fire-and-forget) | Nessun ACK; veloce ma può perdere messaggi. |
| **QoS 1** | **At-Least-Once** *(1 o più volte)* | `PUBLISH` $\to$ `PUBACK` | Ritrasmette finché non arriva `PUBACK`; **possibili duplicati**. |
| **QoS 2** | **Exactly-Once** *(esattamente 1 volta)* | 4-way handshake: `PUBLISH` $\to$ `PUBREC` $\to$ `PUBREL` $\to$ `PUBCOMP` | Nessuna perdita e nessun duplicato, ma latenza maggiore. *(Nota: sulla slide 12 di `2.2` c'è un refuso che ripete "at most once" per QoS2, ma lo schema a 4 messaggi `PUBREC/PUBREL/PUBCOMP` mostra correttamente **Exactly-Once**).* |

### 6.4 Persistent Sessions & Retained Messages
- **Comportamento normale** (`Clean Session = true` / `cleanSession: true`):
  - Quando il client si disconnette, il broker dimentica le sue subscription e non accoda nulla.
- **Persistent Session** (`Clean Session = false` + `clientId` esplicito e fisso!):
  - Il broker salva le subscription del client e **accoda tutti i messaggi QoS 1 e QoS 2** pubblicati mentre il client è offline.
  - Quando il client si riconnette (con lo stesso `clientId`), riceve i messaggi accodati; inoltre ritrasmette eventuali messaggi QoS 1/2 non confermati.
  - **Attenzione**: se un client resta offline troppo a lungo, la coda sul broker può crescere indefinitamente!
- **Retained Message** (`retain = true`):
  - Il broker conserva l'**ultimo** messaggio retained per quel topic e lo consegna immediatamente a ogni nuovo subscriber appena si connette.

### 6.5 Esempio Pratico: Sensor.Community su MQTT
Struttura tipica di un messaggio ricevuto in Node-RED (`msg` completo):
```json
{
  "topic": "/smartcity/milan",
  "payload": {
    "id": 10372898440,
    "timestamp": "2022-05-06 11:46:13",
    "sensordatavalues": [
      { "id": 22999226740, "value_type": "temperature", "value": "16.90" },
      { "id": 22999226768, "value_type": "humidity", "value": "99.90" }
    ],
    "sensor": { "id": 21666, "pin": "7", "sensor_type": { "id": 9, "name": "DHT22" } },
    "location": { "latitude": "45.486", "longitude": "9.19", "country": "IT" }
  },
  "qos": 0,
  "retain": false,
  "_msgid": "..."
}
```
Estrarre la temperatura in un nodo `function`:
```js
const entry = (msg.payload.sensordatavalues || []).find(v => v.value_type === "temperature");
if (!entry) return null;
msg.payload = parseFloat(entry.value); // Attenzione: nel JSON originale "value" è una stringa!
return msg;
```

---

## 7. Nodi e Pattern Aggiuntivi dal Lab 1

### 7.1 Nodo `http request` (HTTP GET / REST API)
- Permette di interrogare API web esterne ogni volta che arriva un messaggio in ingresso (tipicamente da un nodo `inject` manuale o periodico).
- **Configurazione chiave**:
  - `Method`: `GET`
  - `URL`: endpoint completo con query parameter (oppure lasciato vuoto per passare `msg.url` dinamicamente).
  - `Return`: impostare **`a parsed JSON object`** (`ret: "obj"`), così `msg.payload` arriva già come oggetto JS senza dover aggiungere un nodo `json` a valle.
- **Buona pratica**: collega sempre prima un `debug` (`msg.payload` o `complete msg object`) per ispezionare la struttura reale della risposta JSON prima di scrivere il nodo `function` (es. nomi esatti dei campi, tipi stringa vs numero, array annidati).
- **Gestione errori / payload incompleti**: controlla sempre `if (!msg.payload || msg.payload.current == null) return null;` oppure controlla `msg.statusCode === 200` se serve validare la risposta HTTP.

### 7.2 Nodo `openweathermap` (`node-red-node-openweathermap`)
- Nodo di terze parti installabile da **Menu `≡` $\to$ Manage palette $\to$ Install** (`node-red-node-openweathermap`).
- Configurazione:
  - `API Key`: chiave personale (es. in Lab 1: `2caa90098525566a5c251ebb92abd882`).
  - `Weather`: `Current weather for` (`wtype: "current"`) oppure `5 day forecast for` (`wtype: "forecast"`).
  - **Modalità statica**: imposta `City` (es. `Milan`) e `Country` (es. `IT`).
  - **Modalità dinamica** *(un solo nodo per qualsiasi città!)*: lascia vuoti `City`, `Country`, `Lat` e `Lon` nel nodo, e passa in ingresso:
    ```js
    msg.location = { city: "Milan", country: "IT" }; // oppure { lat: 45.46, lon: 9.19 }
    ```
    *(Nota interna di `weather.js`: l'URL HTTP viene costruito in modo sincrono prima di lanciare la richiesta, quindi puoi inviare anche un array di messaggi in raffica verso un singolo nodo `openweathermap` senza race condition.)*
- **Struttura del messaggio prodotto (`current`)**:
  - `msg.payload` è già un oggetto normalizzato dal nodo:
    - `msg.payload.tempc` (temperatura in °C, `number`)
    - `msg.payload.humidity` (umidità %, `number`)
    - `msg.payload.location` (nome della città restituito da OWM — usabile direttamente in Handlebars `{{payload.location}}` nel nodo `Text`!)
    - `msg.payload.weather`, `msg.payload.detail`, `msg.payload.windspeed`, ecc.
  - `msg.location` viene ripopolato con `{ city, country, lat, lon }`.
  - `msg.data` contiene la risposta grezza originale dell'API OpenWeatherMap.
- **Trigger periodico**: collegare a monte un `inject` con `repeat = 60` (ogni minuto) e opzionalmente `once: true` per avere il primo campione subito al Deploy.

### 7.3 `flow` vs `global` tra Catene dello Stesso Tab e Tab Separati
Quando una traccia chiede *"in a separate flow..."*, fai molta attenzione a dove disegni i flussi nell'editor:
- **Due catene sullo STESSO tab**: condividono lo stesso **`flow` context** (`flow.get("k")`, `flow.set("k", v)`). È la scelta più pulita per non sporcare lo spazio globale.
- **Due TAB diversi** (come `Lab1.1`, `Lab1.2`, `Lab1.3` in [`lab/lab1/flows.json`](../lab/lab1/flows.json)): **ogni tab ha il proprio `flow` context isolato!** Se provi a leggere `flow.get("meanTemp")` in un tab diverso da quello in cui hai fatto `flow.set(...)`, otterrai `undefined`. Tra tab diversi devi usare **`global` context** (`global.get("k")`, `global.set("k", v)`).
- **Persistenza opzionale**: con `settings.js` configurato (`nodered-exam`), aggiungi `"file"` come ultimo argomento (`global.set("meanTemp", mean, "file")`) per sopravvivere anche a riavvii e *Full Deploy*.

### 7.4 Log JSON su File: `Overwrite` vs `Append` (NDJSON)
Quando scrivi oggetti JS su file con il nodo `file`, Node-RED serializza automaticamente `msg.payload` in stringa JSON, ma la lettura con `file in` cambia radicalmente a seconda della modalità di scrittura:
1. **Modalità `Overwrite file` (`true`) — Stato/Ultimo valore**:
   - Il file contiene **un solo** valore o oggetto JSON (es. `logHttp.txt`).
   - Lettura: `file in` (`Output = a single utf8 string`) $\to$ `json` (`Always convert to JS Object`) $\to$ `debug` / `function`.
2. **Modalità `Append to file` (`false`) + `Add newline` (`true`) — Storico multi-riga (NDJSON)**:
   - Il file contiene **più** oggetti JSON separati da `\n`.
   - ⚠️ **Trappola**: se leggi con `a single utf8 string` e lo mandi a un nodo `json`, il parser va in errore perché la stringa non è un singolo JSON valido!
   - Soluzione: imposta `file in` su **`one msg per line`** (che emette un messaggio per ogni riga) $\to$ nodo `json` (oppure `function` che fa `lines.filter(Boolean).map(JSON.parse)`).

### 7.5 MQTT Sensor.Community (`mqtt.neslab.it:3200`) — Doppio Formato Payload
Nel Lab 1 il broker del corso (`mqtt.neslab.it`, porta `3200`, `No client ID`, `No auth`, topic `neslabpolimi/smartcity/milan`) pubblica un JSON **bridged** leggermente diverso rispetto all'esempio delle slide (`2.2`):
- **Formato bridge Lab 1 (`neslabpolimi/smartcity/milan`)**: i valori sono dentro **`msg.payload.fields`**:
  ```json
  {
    "fields": {
      "temperature": 18.4,
      "humidity": 62.1,
      "pm25": 12.3,
      "pm10": 19.8
    }
  }
  ```
- **Formato grezzo slide 2.2 (`/smartcity/milan`)**: i valori sono dentro l'array **`msg.payload.sensordatavalues`** come `{ value_type, value }` (con `value` stringa!).
- **Attenzione ai sensori parziali**: non tutte le centraline inviano tutte e 4 le misure in ogni pacchetto (alcune hanno solo temperatura/umidità, altre solo PM2.5/PM10). Controlla sempre `rawVal !== undefined && rawVal !== null && !Number.isNaN(Number(rawVal))` prima di aggiornare max/min/medie.

---

## 8. Guida Completa agli Esercizi del Lab 1 (Soluzioni, Codice e Trappole)
*(Rif: Slide [`Node-RED Lab 1.pdf`](../lab/lab1/Node-RED%20Lab%201.pdf) e flow esportati in [`lab/lab1/flows.json`](../lab/lab1/flows.json))*

### 8.1 Mappa Riepilogativa degli Esercizi

| Esercizio | Tab in `flows.json` | Nodi Principali | Concetto Chiave / Cosa Verificare |
| :--- | :--- | :--- | :--- |
| **Ex 0 — Warm Up** | `Ex 0 - Warm Up` | `inject` $\to$ `openweathermap` $\to$ `switch` $\to$ `change` $\to$ `debug` | Ispezionare `msg.payload` (`tempc`, `humidity`); confronto soglia (`payload.tempc > 15` vs `otherwise`) senza scrivere JS. |
| **Ex 1 — Trend Temp & Humidity** | *(non presente come tab separato)* | `inject` (`60s`) $\to$ `openweathermap` $\to$ `function` (`Outputs = 2`) $\to$ 2× `debug` | Salvare `prevTemp` e `prevHum` in `context`; restituire `[tempMsg, humMsg]` con `"high"`, `"low"` o `"equal"`; gestire il 1° campione (`undefined`). |
| **Ex 2 — Mean & Alert via File** | `Lab1.1`, `Lab1.2`, `Lab1.3` | Flow 1: `inject` (`60s`) $\to$ `openweathermap` $\to$ `function` $\to$ `file`<br>Flow 2: `inject` (`120s`) $\to$ `file in` $\to$ `debug`<br>Flow 3: `inject` $\to$ `file in` $\to$ `function` $\to$ `debug` | **File-based state sharing**: in `flows.json` il valore precedente usa `context`, ma la variante pura usa il file per salvare `{ prevTemp, mean }`. **Bug in `Lab1.3`**: l'`inject` non ha `repeat` (gira solo al boot) e `AllertFilter` non fa `parseFloat()`. |
| **Ex 3 — Refactor con Context Scope** | *(variante di Ex 2)* | Stessi 3 flow di Ex 2 senza `file` / `file in` | **`context`** per `prevTemp` (privato del nodo 1), **`flow`** se i 3 flussi sono sullo stesso tab oppure **`global`** se sono su 3 tab diversi! |
| **Ex 4 — MQTT Max Tracker** | `Lab1.4` | `mqtt in` (`mqtt.neslab.it:3200`, `neslabpolimi/smartcity/milan`) $\to$ `function` $\to$ `debug` | Inizializzare i massimi a `0` (`context.get(k) ?? 0`); gestire campi assenti (`null`/`undefined`) per sensori diversi; stampare solo quando almeno un massimo cresce (`if (!changed) return null`). |
| **Ex 5 — HTTP Pollen Log** | `Lab1.5` + `Lab1.6` | Flow 1: `inject` (`120s`) $\to$ `http request` (`ret: obj`) $\to$ `function` $\to$ `file`<br>Flow 2: `inject` (`240s`) $\to$ `file in` $\to$ `json` $\to$ `debug` | `Return = a parsed JSON object`. **Note su `flows.json`**: in `Lab1.5` `repeat` è `60` invece di `120`, e in `PollenFilter` c'è il refuso `__msgid__` invece di `_msgid`. `Lab1.6` in `flows.json` è la 2ª parte dell'Ex 5! |
| **Ex 6 — HTTP UV Index & Forecast** | *(da completare)* | `inject` (manuale) $\to$ `http request` (`ret: obj`) $\to$ `function` (`Outputs = 2`) $\to$ `debug` + `switch`/`file` | Separare `msg.payload.now.uvi` da `msg.payload.forecast`; filtrare `forecast` **solo per la data odierna** (`rest of the day`) per calcolare la media; loggare su file se `now.uvi > threshold`. |

---

### 8.2 Soluzioni & Snippet Pronti per Ogni Esercizio

#### Exercise 1 — Multi-Output Function Node per Temperatura & Umidità
Configura il nodo `function` con **Setup $\to$ `Outputs = 2`** (uscita 1 = temperatura, uscita 2 = umidità):
```js
const temp = msg.payload.tempc;
const hum  = msg.payload.humidity;

const prevTemp = context.get("prevTemp");
const prevHum  = context.get("prevHum");

context.set("prevTemp", temp);
context.set("prevHum", hum);

// Al primo campione non c'è un valore precedente con cui confrontare
if (prevTemp === undefined || prevHum === undefined) {
    return [null, null];
}

function compare(curr, prev) {
    if (curr > prev) return "high";
    if (curr < prev) return "low";
    return "equal";
}

return [
    { topic: "temperature", payload: compare(temp, prevTemp), _msgid: msg._msgid },
    { topic: "humidity",    payload: compare(hum,  prevHum),  _msgid: msg._msgid }
];
```

#### Exercise 2 — Media Temperatura & Alert tramite File (Variante 100% File-Based + Fix `Lab1.3`)
Se vuoi seguire alla lettera *"Use a file to compute the mean and store any useful state"*, salva un oggetto JSON `{ "prevTemp": ..., "mean": ... }` nel file (es. `weather_state.json` con `Overwrite file = true`):

- **Flow 1 (ogni 60s)**: `inject (60s)` $\to$ `openweathermap` $\to$ `function (salva temp in msg.currTemp)` $\to$ `file in (leggi JSON)` $\to$ `json (to Object)` $\to$ `function (calcola media)` $\to$ `file (overwrite)`:
  ```js
  // Alternativa più semplice usata in Lab1.1: context per prevTemp + file average.txt per la media
  const newTemp = msg.payload.tempc;
  const oldTemp = context.get("oldTemp");
  context.set("oldTemp", newTemp);

  if (oldTemp === undefined) {
      return null; // oppure usa newTemp come prima stima, come in Lab1.1
  }
  msg.payload = (oldTemp + newTemp) / 2;
  return msg;
  ```
- **Flow 3 (Alert > 10°C — versione corretta di `Lab1.3`)**:
  - Imposta **`repeat`** sull'`inject` (es. `60` secondi) invece di lasciarlo solo `once: true`!
  - Converti esplicitamente la stringa letta da `file in`:
  ```js
  const avg = parseFloat(msg.payload);
  if (Number.isNaN(avg) || avg <= 10) {
      return null;
  }
  msg.payload = `ALERT: mean temperature is ${avg.toFixed(2)} °C (> 10°C)`;
  return msg;
  ```

#### Exercise 3 — Refactor con Context Scope (`context` + `flow` / `global`)
Usa `global` se i 3 flussi sono in **tab diversi** (oppure `flow` se li metti sullo **stesso tab**):
- **Flow 1 (ogni 60s — `Compute Mean`)**:
  ```js
  const curr = msg.payload.tempc;
  const prev = context.get("prevTemp"); // privato del singolo nodo
  context.set("prevTemp", curr);

  if (prev === undefined) return null;

  const mean = (prev + curr) / 2;
  global.set("meanTemp", mean); // usa flow.set(...) se stesso tab!
  msg.payload = mean;
  return msg;
  ```
- **Flow 2 (ogni 120s — `Print Mean`)**:
  ```js
  const mean = global.get("meanTemp");
  if (mean === undefined) return null;
  msg.payload = mean;
  return msg;
  ```
- **Flow 3 (periodico, es. ogni 60s — `Check Alert > 10°C`)**:
  ```js
  const mean = global.get("meanTemp");
  if (mean === undefined || mean <= 10) return null;
  msg.payload = `ALERT: meanTemp = ${mean.toFixed(2)} °C`;
  return msg;
  ```

#### Exercise 4 — MQTT Max Tracker su `mqtt.neslab.it:3200`
- **Configurazione `mqtt in`**:
  - Broker: `mqtt.neslab.it`, Porta: `3200`, `Client ID`: vuoto, `Clean Session`: `true`.
  - Topic: `neslabpolimi/smartcity/milan`, `Output`: `auto-detect` (o `a parsed JSON object`).
- **Nodo `function` (compatibile con `fields` di Lab 1 e con eventuale `sensordatavalues`)**:
  ```js
  if (typeof msg.payload === "string") {
      try { msg.payload = JSON.parse(msg.payload); } catch (e) { return null; }
  }

  const fields = msg.payload && msg.payload.fields;
  if (!fields) return null;

  function updateMax(key, rawVal) {
      if (rawVal === undefined || rawVal === null) return false;
      const val = Number(rawVal);
      if (Number.isNaN(val)) return false;

      const currentMax = context.get(key) ?? 0; // assunto >= 0 dalla traccia
      if (val > currentMax) {
          context.set(key, val);
          return true;
      }
      return false;
  }

  let changed = false;
  if (updateMax("maxTemp", fields.temperature)) changed = true;
  if (updateMax("maxHum",  fields.humidity))    changed = true;
  if (updateMax("maxPm25", fields.pm25))        changed = true;
  if (updateMax("maxPm10", fields.pm10))        changed = true;

  if (!changed) return null;

  msg.payload = {
      temperature: context.get("maxTemp") ?? 0,
      humidity:    context.get("maxHum")  ?? 0,
      pm25:        context.get("maxPm25") ?? 0,
      pm10:        context.get("maxPm10") ?? 0
  };
  return msg;
  ```

#### Exercise 5 — HTTP GET Open-Meteo Pollen & Log su File
- **Endpoint**: `https://air-quality-api.open-meteo.com/v1/air-quality?latitude=45.4643&longitude=9.1895&current=alder_pollen,birch_pollen,grass_pollen,mugwort_pollen,olive_pollen,ragweed_pollen&forecast_days=1`
- **Struttura risposta**: `msg.payload.current` contiene `{ time, interval, alder_pollen, birch_pollen, grass_pollen, mugwort_pollen, olive_pollen, ragweed_pollen }` e `msg.payload.current_units` le unità (`grains/m³`).
- **Flow 1 (`inject repeat = 120s` $\to$ `http request [ret=obj]` $\to$ `PollenFilter` $\to$ `file`)**:
  ```js
  const pollenValue = msg.payload?.current;
  const pollenUnits = msg.payload?.current_units;
  if (!pollenValue || !pollenUnits) return null;

  return {
      _msgid: msg._msgid, // Attenzione: in flows.json era scritto "__msgid__" (refuso)
      topic: msg.topic,
      payload: {
          current: pollenValue,
          current_units: pollenUnits
      }
  };
  ```
- **Flow 2 (`inject repeat = 240s` $\to$ `file in` $\to$ `json` $\to$ `debug`)**:
  - Se il nodo `file` in Flow 1 usa `Overwrite file = true`, `file in` (`utf8 string`) + `json` restituisce l'oggetto JS dell'ultimo log.
  - Se invece il nodo `file` usa `Append to file = false` + `Add newline`, imposta `file in` su **`one msg per line`** prima del nodo `json`!

#### Exercise 6 — HTTP GET Current UV Index (`now` vs `forecast` del resto della giornata)
- **Endpoint**: `https://currentuvindex.com/api/v1/uvi?latitude=45.4643&longitude=9.1895`
- **Struttura reale del JSON restituito**:
  ```json
  {
    "ok": true,
    "latitude": 45.4643,
    "longitude": 9.1895,
    "now": { "time": "2026-11-17T17:00:00Z", "uvi": 0 },
    "forecast": [
      { "time": "2026-11-17T18:00:00Z", "uvi": 0 },
      { "time": "2026-11-18T00:00:00Z", "uvi": 0 }
    ]
  }
  ```
  > ⚠️ **Attenzione**: `forecast` contiene le ore dei **prossimi giorni** (non solo oggi)! Per calcolare *"the mean value for the rest of the day"*, devi filtrare solo le entry di `forecast` con la **stessa data** di `now.time` (i primi 10 caratteri ISO `YYYY-MM-DD`).
- **Architettura consigliata**:
  `inject (manuale)` $\to$ `http request (GET, ret = parsed JSON object)` $\to$ `function (Outputs = 2)`:
  - **Uscita 1** (`now.uvi`): collegata sia a `debug` sia a un `switch` (`msg.payload > 3` [soglia manuale]) $\to$ `file` (log dell'indice su file).
  - **Uscita 2** (`forecastMeanToday`): collegata a `debug`.
- **Codice del nodo `function` (`Outputs = 2`)**:
  ```js
  const now = msg.payload?.now;
  const forecast = msg.payload?.forecast;
  if (!now || !Array.isArray(forecast)) return [null, null];

  const today = now.time.slice(0, 10); // es. "2026-11-17"
  const restOfDay = forecast.filter(
      entry => typeof entry.time === "string" &&
               entry.time.slice(0, 10) === today &&
               entry.time > now.time
  );

  const forecastMean = restOfDay.length > 0
      ? restOfDay.reduce((sum, e) => sum + Number(e.uvi), 0) / restOfDay.length
      : null;

  const currentMsg = {
      _msgid: msg._msgid,
      topic: "uv/current",
      payload: now.uvi
  };

  const forecastMsg = {
      _msgid: msg._msgid,
      topic: "uv/forecast_mean_today",
      payload: forecastMean !== null ? Number(forecastMean.toFixed(3)) : "No remaining hours today"
  };

  return [currentMsg, forecastMsg];
  ```

---

## 9. Nodi e Pattern dal Lab 2 — Chatbot & Telegram (`node-red-contrib-chatbot`)

### 9.1 Creazione e Configurazione Bot Telegram (`@BotFather`)
1. Su Telegram apri `@BotFather` (`t.me/BotFather`) e invia `/newbot`.
2. Scegli un **Name** (es. `Node-RED Lab`) e uno **username** che termini obbligatoriamente con `bot` (es. `nodered_lab_bot`).
3. Copia il **Token HTTP API** generato da BotFather.
4. In Node-RED (nodi `Telegram In` / `chatbot-telegram-receive` e `Telegram Out` / `chatbot-telegram-send`):
   - Configura **`Bot configuration (development)`** (`chatbot-telegram-node`) inserendo **Bot Name** e **Token**.
   - Lascia `Connect Mode = Polling` (`1000 ms`): in polling il bot funziona in locale senza IP pubblico né certificati HTTPS/webhook.
   - *(Nota)*: `Bot configuration (production)` viene usata solo se la variabile globale `"environment"` in `settings.js` è impostata a `"production"`; altrimenti Node-RED usa sempre `development`.

### 9.2 Struttura del Messaggio RedBot (`msg`) & Trappola nei `function` Node
Quando un utente scrive su Telegram, `chatbot-telegram-receive` emette un `msg` con questa struttura:
- `msg.payload`:
  ```json
  {
    "type": "message",
    "content": "What's your name?",
    "chatId": 123456789,
    "messageId": 42,
    "inbound": true
  }
  ```
- `msg.originalMessage`: messaggio originale Telegram (`from.username`, `from.first_name`, `from.last_name`, `chat.id`, `text`, ecc.).
- `msg.chat()`: funzione helper per accedere al *chat context* dell'utente corrente (`await msg.chat().get("...")`, `msg.chat().set("...", val)`).

> ⚠️ **Trappola classica nel nodo `function` (senza nodo `Text`)**:
> Se modifichi `msg.payload.content` direttamente in un nodo `function` e lo mandi subito a `chatbot-telegram-send` (come in [`exercise8-0.json`](../lab/lab2/exercise8-0.json)), **devi ripulire i flag di ingresso**, altrimenti `Telegram Out` può ignorare o gestire male il pacchetto:
> ```js
> msg.payload.content = "I am the NSDS Telegram Bot!";
> delete msg.ts;
> delete msg.transport;
> msg.payload.inbound = false;
> msg.payload.fallback = null;
> return msg;
> ```
> **Alternativa più pulita**: usare il nodo **`Text`** (`chatbot-message`), che costruisce automaticamente il payload outbound valido per qualsiasi piattaforma!

### 9.3 Nodo `Text` (`chatbot-message`) & Sintassi Handlebars
Come suggerito dall'*Hint* di Exercise 5-1, il nodo **`Text`** (`chatbot-message` nella palette RedBot) è molto più potente di un testo statico:
- **Preserva il contesto di chat anche se un nodo intermedio sovrascrive `msg.payload`**: ad esempio, se colleghi `chatbot-telegram-receive` $\to$ `function` $\to$ `openweathermap` $\to$ `chatbot-message` (`Text`) $\to$ `chatbot-telegram-send`:
  - `openweathermap` sostituisce `msg.payload` con `{ tempc, temp_minc, temp_maxc, humidity, ... }`, ma **mantiene intatti** `msg.originalMessage` e `msg.chat()`.
  - `Text` (`chatbot-message`) legge comunque `chatId` / `messageId` da `msg.originalMessage` e `msg.chat()`, e ricostruisce il payload `{ type: "message", content: "...", chatId, inbound: false }`!
- **Variabili Handlebars supportate nel testo**:
  - Campi del payload a monte: `{{payload.tempc}}`, `{{payload.temp_minc}}`, `{{payload.temp_maxc}}`, `{{payload.humidity}}`, ecc.
  - Campi automatici del chat context: `{{firstName}}`, `{{lastName}}`, `{{userId}}`, `{{chatId}}`, `{{transport}}`, `{{language}}`.
  - Esempio (usato in [`lab/lab2/flows.json`](../lab/lab2/flows.json)):
    ```text
    The current temperature in Milan is {{payload.tempc}}C
    ```
- **Multi-version & Emoji**: puoi aggiungere più varianti di messaggio (una viene scelta casualmente) e usare shortcode emoji come `:smile:`.

### 9.4 Nodo `Rules` (`chatbot-rules`) vs `function`
- **`chatbot-rules`** (categoria `RedBot Flow`): instrada i messaggi su uscite multiple senza scrivere JS in base a:
  - `command` (match esatto, es. `/start`, `/status`) o `commandStartsWith`
  - `messageType` (`message`, `location`, `image`, `audio`, ...)
  - `hasVariable` / `isVariable` (su variabili del chat context)
  - `catchAll` (ultima regola di fallback).
- **Quando preferire `function`**: per frasi in linguaggio naturale o con parametri variabili nel testo (es. `"What is the temperature in Milan?"` o `"My name is Luca!"`), usare un nodo `function` con `Outputs = N` e regex/confronto stringhe è molto più diretto.

---

## 10. Guida Completa agli Esercizi del Lab 2 (Soluzioni, Codice e Trappole)
*(Rif: Slide [`Node-RED Lab 2.pdf`](../lab/lab2/Node-RED%20Lab%202.pdf), [`lab/lab2/exercise8-0.json`](../lab/lab2/exercise8-0.json) e [`lab/lab2/flows.json`](../lab/lab2/flows.json))*

### 10.1 Mappa Riepilogativa degli Esercizi

| Esercizio | Tab / File di Riferimento | Nodi Principali | Concetto Chiave / Cosa Verificare |
| :--- | :--- | :--- | :--- |
| **Ex 5-0 — Bot Base (Example)** | `Exercise 5-0` ([`exercise8-0.json`](../lab/lab2/exercise8-0.json)) | `chatbot-telegram-receive` $\to$ `function` $\to$ `chatbot-telegram-send` (+ `debug`) | Controllo su `msg.payload.content`; pulizia obbligatoria `delete msg.ts; delete msg.transport; msg.payload.inbound = false; msg.payload.fallback = null;`. |
| **Ex 5-1 — Bot Meteo Milano/Roma** | `Exercise 5-0` in [`lab/lab2/flows.json`](../lab/lab2/flows.json) *(Milano già fatta)* | `chatbot-telegram-receive` $\to$ `function` (`Outputs = 3`) $\to$ 2× `openweathermap` $\to$ 2× `chatbot-message` (`Text`) $\to$ `chatbot-telegram-send` | In `flows.json` è implementato il ramo Milano (`MilanOW` $\to$ `WeatherResponse`); per completare Ex 5-1 aggiungi il 2° ramo `openweathermap` per **Rome, IT** (porta dedicata sul `function`). |
| **Ex 5-2 — Saluto + Log Persistente Utenti** | *(estensione di Ex 5-1)* | `function` (parser `"My name is ..."`) + `flow.get/set(..., "file")` **oppure** nodo `file` (`Append`) + `chatbot-message` / `chatbot-telegram-send` | Estrarre il nome (`"My name is Luca!"` $\to$ `"Hello Luca!"`) e salvare in modo persistente i nomi/username visti finora tra riavvii e deploy. |

---

### 10.2 Soluzioni & Snippet Pronti per Ogni Esercizio

#### Exercise 5-0 — Bot Q&A Base (`exercise8-0.json`)
```js
if (msg.payload.content === "What's your name?") {
    msg.payload.content = "I am the NSDS Telegram Bot!";
} else {
    msg.payload.content = "I don't get what you're asking...";
}
delete msg.ts;
delete msg.transport;
msg.payload.inbound = false;
msg.payload.fallback = null;

return msg;
```

#### Exercise 5-1 — Aggiungere Temperatura Corrente per Milan & Rome
- **Architettura consigliata** (`Outputs = 3` sul nodo `function`):
  - **Uscita 1**: `openweathermap` (`City = Milan`, `Country = IT`) $\to$ `chatbot-message` (`Text`: `"The current temperature in Milan is {{payload.tempc}}C"`) $\to$ `chatbot-telegram-send`
  - **Uscita 2**: `openweathermap` (`City = Rome`, `Country = IT`) $\to$ `chatbot-message` (`Text`: `"The current temperature in Rome is {{payload.tempc}}C"`) $\to$ `chatbot-telegram-send`
  - **Uscita 3 (Fallback / Risposte testuali dirette)**: `chatbot-telegram-send`
- **Codice del nodo `function` (`Outputs = 3`)**:
  ```js
  const text = (msg.payload?.content || "").trim();
  const normalized = text.toLowerCase();

  if (normalized === "what's your name?") {
      msg.payload.content = "I am the NSDS Telegram Bot!";
  } else if (normalized === "what is the temperature in milan?") {
      return [msg, null, null];
  } else if (normalized === "what is the temperature in rome?") {
      return [null, msg, null];
  } else {
      msg.payload.content = "I don't get what you're asking...";
  }

  delete msg.ts;
  delete msg.transport;
  msg.payload.inbound = false;
  msg.payload.fallback = null;
  return [null, null, msg];
  ```
  > 💡 **Nota**: collegando `openweathermap` al nodo `Text` (`chatbot-message`), non devi ricostruire manualmente `inbound = false` o cancellare `msg.ts`: ci pensa il nodo `Text` e sostituisce `{{payload.tempc}}` col valore numerico della temperatura!

#### Exercise 5-2 — Saluto Personalizzato & Log Persistente degli Usernames
Estendi il `function` di Ex 5-1 aggiungendo una **4ª uscita** collegata a un nodo `file` (`Append to file = false`, `Add newline = true`, es. `lab/lab2/data/usernames.txt`) **oppure** usando direttamente il persistent context store `"file"` (`localfilesystem` già attivo in `settings.js`):

- **Codice del nodo `function` (`Outputs = 4`: `[MilanOW, RomeOW, TelegramSend, FileLog]`)**:
  ```js
  const text = (msg.payload?.content || "").trim();
  const normalized = text.toLowerCase();

  if (normalized === "what's your name?") {
      msg.payload.content = "I am the NSDS Telegram Bot!";
  } else if (normalized === "what is the temperature in milan?") {
      return [msg, null, null, null];
  } else if (normalized === "what is the temperature in rome?") {
      return [null, msg, null, null];
  } else {
      const match = text.match(/^my name is\s+(.+?)[!.]?$/i);
      if (match) {
          const userName = match[1].trim();
          const tgUsername = msg.originalMessage?.from?.username || null;

          // 1) Persistenza in context store "file" (lista deduplicata su disco)
          const seen = flow.get("seenUsernames", "file") || [];
          const entry = tgUsername ? `${userName} (@${tgUsername})` : userName;
          if (!seen.includes(entry)) {
              seen.push(entry);
              flow.set("seenUsernames", seen, "file");
          }

          // 2) Risposta su Telegram (uscita 3) + riga di log su file (uscita 4)
          msg.payload.content = `Hello ${userName}!`;
          delete msg.ts;
          delete msg.transport;
          msg.payload.inbound = false;
          msg.payload.fallback = null;

          const logMsg = {
              payload: `${new Date().toISOString()} - ${entry}`
          };
          return [null, null, msg, logMsg];
      }

      if (normalized === "/users") {
          const seen = flow.get("seenUsernames", "file") || [];
          msg.payload.content = seen.length > 0
              ? `Seen users: ${seen.join(", ")}`
              : "No users seen yet.";
      } else {
          msg.payload.content = "I don't get what you're asking...";
      }
  }

  delete msg.ts;
  delete msg.transport;
  msg.payload.inbound = false;
  msg.payload.fallback = null;
  return [null, null, msg, null];
  ```
  > ⚠️ **Attenzione ai tipi di messaggio**:
  > - Su Telegram un utente potrebbe inviare sticker, foto o posizione (`msg.payload.content` non è una stringa): usare `(msg.payload?.content || "")` (o `typeof msg.payload?.content === "string"`) evita errori `TypeError` nel nodo `function`.
  > - Usando **sia** `flow.set("seenUsernames", seen, "file")` **sia** il nodo `file` in `Append`, sei coperto qualunque modalità di persistenza venga richiesta all'esame!

---

## 11. Workspace Separati & Configurazioni di Esecuzione

Per evitare che contesti su disco (`context/`), file dati (`data/`) o database (`*.sqlite`) dei laboratori interferiscano con l'esame o con le esercitazioni extra, i workspace sono completamente isolati:

| Configurazione (IntelliJ IDEA) | Comando PowerShell (`nodered/`) | `userDir` (Isolata) | Porta Web | Scopo |
| :--- | :--- | :--- | :--- | :--- |
| **`Node-RED Exam`** | `.\start-nodered.ps1 -Mode exam` | `nodered-exam/` | **`1880`** | Ambiente pulito dedicato all'esame |
| **`Node-RED Practice (Exercises)`** | `.\start-nodered.ps1 -Mode practice` | `nodered-practice/` | **`1881`** | Ambiente separato per esercitarsi (avviabile anche insieme a Exam) |
| **`Node-RED Lab 1`** | `.\start-nodered.ps1 -Mode lab1` | `lab/lab-userdir/` | `1880` | Soluzioni/flow del Lab 1 |
| **`Node-RED Lab 2`** | `.\start-nodered.ps1 -Mode lab2` | `lab/lab-userdir/` | `1880` | Soluzioni/flow del Lab 2 |
| **`Node-RED Templates (All Lectures)`** | `.\start-nodered.ps1 -Mode templates` | `nodered-lab-template/` | `1880` | Tutti i template delle 5 lezioni |

---

## 12. Case Study d'Esame — Telegram Weather & Wind Forecast Bot (Evaluation Lab)
*(Riferimento pratico implementato in [`nodered-practice/flows.json`](../nodered-practice/flows.json))*

### 12.1 Requisiti Tipici & Architettura Consigliata (11 nodi)
Requisiti della traccia:
1. Rispondere su Telegram a **2 tipi di query** (meteo `weather[y].description` e velocità vento `wind.speed`) per **2 città** (`Milano` / `Rome`) a **1 o 2 giorni** usando OpenWeatherMap.
2. Indicare per ogni query se la previsione è **cambiata o rimasta uguale** rispetto all'ultima volta che è stata chiesta **la stessa informazione** (indipendentemente dall'utente).
3. Salvare **ogni minuto** su un file locale quante query **di ciascun tipo** sono state servite nell'ultimo minuto.
4. Formattare sempre le risposte (non inviare valori grezzi) e assumere esecuzione **24/7**.

**Architettura pulita (zero duplicazione, 1 solo `Filter`, 1 solo `Telegram Out`, logger disaccoppiato):**
```text
[Telegram In] ─▶ [Parser (3 out)] ──(1)──▶ [MilanOW (forecast)] ─┐
                                  ──(2)──▶ [RomeOW (forecast)]  ─┼─▶ [Filter] ─▶ [Output ({{payload.reply}})] ─┐
                                  ──(3)──▶ [Error (fallback)] ─────────────────────────────────────────────────┼─▶ [Telegram Out]

[Inject (60s, once: false)] ─▶ [Reset & format log] ─▶ [File (log.txt, path relativo)]
```

> 💡 **Perché un solo `Filter` + logger separato?**
> Evita di duplicare selettori e filtri per città/tipo (principale fonte di bug da copia-incolla), usa `flow` context per separare il timer dal parsing Telegram, e rende semplicissima la descrizione dei flussi entro le **200 parole** nel `README.md`.

---

### 12.2 Pattern 1 — Tokenizzazione & Parsing Robusto (`Parser`, `Outputs = 3`)
Mai fare confronti su stringhe esatte (apostrofi curvi `’` vs `'`, maiuscole, `?`, `Milan` vs `Milano`) e proteggersi sempre da messaggi non testuali (sticker/foto):

```js
// Tokenizzazione sicura: minuscolo + solo sequenze alfanumeriche (elimina punteggiatura e spazi doppi)
const raw = typeof msg.payload?.content === "string" ? msg.payload.content : "";
const tokens = raw.toLowerCase().match(/[a-z0-9]+/g) || [];

// 1) Città
let city = null;
if (tokens.includes("milan") || tokens.includes("milano")) city = "Milano";
else if (tokens.includes("rome") || tokens.includes("roma")) city = "Rome";

// 2) Tipo query: controllare SEMPRE "wind" prima di "forecast" (es. "wind forecast")
let queryType = null;
if (tokens.includes("wind")) queryType = "wind";
else if (tokens.includes("weather") || tokens.includes("forecast")) queryType = "weather";

// 3) Giorni (1 = tomorrow, 2 = in two days)
let days = -1;
if (tokens.includes("tomorrow") || tokens.includes("one") || tokens.includes("1")) days = 1;
else if (tokens.includes("two") || tokens.includes("2")) days = 2;

if (city === null || queryType === null || days === -1) {
    return [null, null, msg]; // ramo Error / fallback
}

// Metadati su msg: sopravvivono al nodo openweathermap (che sovrascrive solo msg.payload e msg.location)
msg.city = city;
msg.queryType = queryType;
msg.days = days;

return city === "Milano" ? [msg, null, null] : [null, msg, null];
```

> ⚠️ **Trappole JS nella tokenizzazione**:
> - `.trim()` restituisce una **stringa**, non un array (serve `.split(/\s+/)` dopo pulizia punteggiatura oppure `.match(/[a-z0-9]+/g) || []`).
> - `for (let x in arr)` itera sugli **indici** (`"0"`, `"1"`, ...); per i valori usa **`for (let x of arr)`** o `arr.includes("...")`.

---

### 12.3 Pattern 2 — OpenWeatherMap `5 day forecast` (`wtype: "forecast"`)
- Con **`current`**: `msg.payload` è un **oggetto singolo** del meteo attuale (`tempc`, `detail`, `windspeed`). **Sbagliato** per previsioni a 1–2 giorni!
- Con **`forecast`**: `msg.payload` è un **array di 40 elementi** (slot ogni **3 ore** $\to$ 8 slot/giorno $\times$ 5 giorni). Ogni elemento `entry = msg.payload[x]` contiene:
  - `entry.weather[0].description` (es. `"few clouds"`)
  - `entry.wind.speed` (es. `0.35`, in m/s)
  - `entry.dt` (timestamp Unix in secondi) e `entry.dt_txt` (es. `"2026-11-20 12:00:00"` UTC)

**Selezione stabile del giorno target (`oggi + msg.days`):**
```js
const target = new Date();
target.setUTCDate(target.getUTCDate() + msg.days);
const targetDateStr = target.toISOString().slice(0, 10); // "YYYY-MM-DD"

// Preferisce lo slot delle 12:00:00 UTC del giorno target (evita oscillazioni al cambio finestra 3h)
const entry =
    list.find(item => item.dt_txt === `${targetDateStr} 12:00:00`) ||
    list.find(item => item.dt_txt && item.dt_txt.startsWith(targetDateStr)) ||
    list[Math.min(msg.days * 8 - 1, list.length - 1)];
```
*(Rispetto agli indici fissi `payload[8]` / `payload[16]`, ancorarsi alle `12:00:00` di `targetDateStr` evita falsi "changed" o "nuova query" quando scatta lo slot da 3 ore.)*

---

### 12.4 Pattern 3 — Change Detection Multi-Chiave & Formattazione (`Filter`)
Per distinguere tutte le combinazioni `(città, tipo, giorno)` e funzionare **24/7** anche al cambio di data, usa una mappa in `context` con chiave `${city}_${queryType}_${targetDateStr}`:

```js
// On Start:
context.set("history", {});
```
```js
// On Message (dopo aver estratto entry e currentValue):
// Incrementa il contatore per tipo SOLO dopo che OpenWeatherMap ha risposto
const counts = flow.get("queryCounts") || { weather: 0, wind: 0 };
counts[msg.queryType] += 1;
flow.set("queryCounts", counts);

const key = `${msg.city}_${msg.queryType}_${targetDateStr}`;
const history = context.get("history") || {};
const previousValue = history[key];

let changeNotice;
if (previousValue === undefined) {
    changeNotice = "This is the first time I am asked this!";
} else if (previousValue === currentValue) {
    changeNotice = "It is the same as the last time I was asked!";
} else {
    changeNotice = "It changed since the last time I was asked!";
}

history[key] = currentValue;
context.set("history", history);

// Sostituisci l'array msg.payload con un oggetto pulito per il nodo Text ({{payload.reply}})
msg.payload = { reply: `${answer} ${changeNotice}` };
return msg;
```

> ⚠️ **Trappole classiche su `context` e `payload`**:
> 1. **Valore `0` falsy (`wind.speed === 0`)**: usare `if (previousValue === undefined)`, **MAI** `if (previousValue)` — altrimenti con vento a `0` m/s il ramo salta!
> 2. **Post-incremento**: `context.set("n", n++)` salva il **vecchio** valore di `n` (rimane fermo a `0`!). Scrivere sempre `context.set("n", n + 1)`.
> 3. **Payload Array**: in modalità `forecast`, `msg.payload` è un Array; sostituiscilo con `{ reply: "..." }` (RedBot mantiene comunque `msg.originalMessage` e `msg.chat()` per rispondere all'utente giusto).

---

### 12.5 Pattern 4 — Statistiche Ogni Minuto su File (`flow` context + `file`)
- **`inject`**: `repeat = 60` secondi, `once = false` (evita una scrittura a `0` subito dopo il deploy).
- **`function` (`Reset & format log`)**:
  ```js
  // On Start:
  flow.set("queryCounts", { weather: 0, wind: 0 });

  // On Message:
  const counts = flow.get("queryCounts") || { weather: 0, wind: 0 };
  flow.set("queryCounts", { weather: 0, wind: 0 }); // reset per il minuto successivo

  msg.payload = `Weather: ${counts.weather}\nWind: ${counts.wind}`;
  return msg;
  ```
- **Nodo `file`**:
  - Usa **sempre un percorso relativo** (es. `log.txt`), mai `C:\Users\...` (altrimenti fallisce sulla macchina del docente).
  - `overwriteFile: "true"` è ideale per sistemi 24/7 (mantiene sempre il conteggio dell'ultimo minuto senza far esplodere la dimensione del file); usa `append` + timestamp solo se la traccia chiede lo storico completo.

---

### 12.6 Checklist Pre-Consegna (Regole anti-penalità `-1` punto)
- [ ] **Bot config**: imposta lo stesso config node sia in `bot` (development) sia in `botProduction` su `Telegram In` e `Telegram Out`.
- [ ] **Un solo tab pulito** (rinominato `nodered-groupXX`): rimuovi tab di test/bozze, broker MQTT inutilizzati, nodi `comment` e `debug` superflui.
- [ ] **Zip `nodered-groupXX.zip`** contenente **solo** `flows.json` e `README.md`.
- [ ] **`README.md`** completo con: Group ID, nomi dei membri, descrizione dei flussi (**$\le$ 200 parole**: chi manda cosa a chi e quando), eventuali estensioni extra oltre a quelle dei lab, URL del bot (`https://t.me/...`), API key del Telegram Bot.

---

## 13. Riferimento Rapido JS — Parsing di Stringhe, Tokenizzazione & Regex

> **Regola fondamentale**: in JavaScript le stringhe sono **immutabili**. Nessun metodo modifica la stringa originale; tutti restituiscono una **nuova stringa** o un nuovo valore (array, booleano, numero).

### 13.1 Pulizia e Normalizzazione (Pre-processing)
Prima di fare parsing o confronti, pulisci quasi sempre spazi superflui, punteggiatura o differenze di maiuscole/minuscole:

| Metodo | Cosa fa | Esempio |
| :--- | :--- | :--- |
| `.trim()` | Rimuove spazi, tab e a capo all'inizio e alla fine | `"  ciao \n".trim()` $\to$ `"ciao"` |
| `.toLowerCase()` / `.toUpperCase()` | Uniforma il case (essenziale per confronti) | `"Milano".toLowerCase()` $\to$ `"milano"` |
| `.replace(a, b)` | Sostituisce la **prima** occorrenza (o **tutte** con regex `/g`) | `"a-b-c".replace(/-/g, " ")` $\to$ `"a b c"` |
| `.replaceAll(a, b)` | Sostituisce tutte le occorrenze di una sottostringa | `"1,2,3".replaceAll(",", ".")` $\to$ `"1.2.3"` |

---

### 13.2 Ricerca ed Estrazione di Sottostringhe
Utili quando la struttura del testo è fissa o basata su delimitatori/posizioni note:

- **Controllo e posizione**:
  - `.includes(sub)` $\to$ `true` / `false` (verifica se contiene `sub`)
  - `.startsWith(sub)` / `.endsWith(sub)` $\to$ `true` / `false`
  - `.indexOf(sub)` $\to$ indice della prima occorrenza, oppure `-1` se assente
- **Taglio (Slicing)** — preferire sempre `.slice()` a `.substring()` perché supporta gli indici negativi:
  ```js
  const str = "JavaScript";
  str.slice(0, 4);   // "Java"   (da 0 a 4 escluso)
  str.slice(4);      // "Script" (da 4 fino alla fine)
  str.slice(-6);     // "Script" (ultimi 6 caratteri)
  ```

---

### 13.3 Tokenizzazione: i 3 Approcci Principali

#### Approccio 1 — Tokenizzazione semplice con `.split()`
Ideale quando i token sono separati da un delimitatore fisso (spazi, virgole, `;`):
```js
const frase = "  Ciao,   come stai oggi?  ";
const tokens = frase.trim().split(/\s+/);
// ["Ciao,", "come", "stai", "oggi?"]
```
- `.trim()` evita elementi vuoti `""` all'inizio/fine.
- `.split(/\s+/)` gestisce spazi multipli e tab (a differenza di `.split(" ")`).
- **Limite**: la punteggiatura resta attaccata alle parole (`"Ciao,"`, `"oggi?"`).

#### Approccio 2 — Tokenizzazione pulita con `.match()` e Regex
Ideale per estrarre parole/numeri puliti ignorando spazi e punteggiatura (oppure separandola):
```js
// A) Solo parole e numeri inglesi (pattern usato nel Parser del Bot Telegram)
const raw = "  Ordine #42: 2 Mele, 1 Pera! ";
const tokens = raw.toLowerCase().match(/[a-z0-9]+/g) || [];
// ["ordine", "42", "2", "mele", "1", "pera"]

// B) Parole + numeri + simboli/punteggiatura come token separati
const testo = "Prezzo: 42 euro! (sconto 10%)";
const tokensFull = testo.match(/\w+|[^\s\w]/g) || [];
// ["Prezzo", ":", "42", "euro", "!", "(", "sconto", "10", "%", ")"]

// C) Supporto lettere accentate italiane (à, è, ò, ...) con flag Unicode 'u'
const tokensIT = "Città: 10€".match(/[\p{L}\p{N}]+/gu) || [];
// ["Città", "10"]
```
**Anatomia di `raw.toLowerCase().match(/[a-z0-9]+/g) || []`**:
1. `raw.toLowerCase()` $\to$ converte tutto in minuscolo (coerente con `a-z`).
2. `[a-z0-9]` $\to$ accetta una lettera minuscola `a`–`z` o una cifra `0`–`9`.
3. `+` $\to$ 1 o più caratteri consecutivi (cattura l'intera parola, non singole lettere).
4. `g` $\to$ flag *global*: restituisce **tutte** le corrispondenze, non solo la prima.
5. `|| []` $\to$ rete di sicurezza: se `.match()` non trova nulla restituisce **`null`**, che viene convertito in `[]`.

#### Approccio 3 — Tokenizzatore tipizzato (Mini-Lexer con `regex.exec()`)
Quando serve conoscere anche il **tipo** di ogni token (`NUMBER`, `WORD`, `OPERATOR`):
```js
function tokenize(input) {
    const regex = /\s*(?:(\d+)|([a-zA-Z]+)|([+\-*/()]))\s*/g;
    const tokens = [];
    let match;

    while ((match = regex.exec(input)) !== null) {
        if (match[1]) tokens.push({ type: "NUMBER", value: Number(match[1]) });
        else if (match[2]) tokens.push({ type: "WORD", value: match[2] });
        else if (match[3]) tokens.push({ type: "OPERATOR", value: match[3] });
    }
    return tokens;
}
// tokenize("x + 12 * 3") -> [{type:"WORD",value:"x"}, {type:"OPERATOR",value:"+"}, ...]
```

---

### 13.4 Espressioni Regolari (Regex) in JavaScript

#### Come crearle
```js
// 1. Sintassi letterale (consigliata per pattern fissi)
const regexFissa = /\d+/g;

// 2. Costruttore RegExp (per pattern dinamici da variabili; raddoppiare i backslash!)
const parola = "rome";
const regexDinamica = new RegExp("\\b" + parola + "\\b", "i");
```

#### Flag principali
| Flag | Nome | Comportamento |
| :--- | :--- | :--- |
| `g` | Global | Trova **tutte** le corrispondenze, non si ferma alla prima |
| `i` | Ignore case | Ignora maiuscole/minuscole (`a` = `A`) |
| `m` | Multiline | `^` e `$` riconoscono inizio/fine di ogni riga |
| `u` | Unicode | Abilita supporto completo ad accenti ed emoji (`\p{L}`, `\p{N}`) |

#### Mattoni della sintassi Regex
| Categoria | Sintassi | Significato |
| :--- | :--- | :--- |
| **Caratteri** | `\d` / `\D` | Una cifra (`0-9`) / Non una cifra |
| | `\w` / `\W` | Lettera, cifra o `_` / Tutto il resto |
| | `\s` / `\S` | Spazio, tab, newline / Non uno spazio |
| | `.` | Qualsiasi carattere tranne a capo (`\.` per punto letterale) |
| **Quantificatori** | `+` / `*` / `?` | `1+` volte / `0+` volte / `0` o `1` volta (opzionale) |
| | `{n}` / `{min,max}` | Esattamente `n` volte / tra `min` e `max` volte (es. `\d{4}`) |
| **Ancore** | `^` / `$` | Inizio stringa / Fine stringa |
| | `\b` | Confine di parola (*word boundary*, es. `\brome\b`) |
| **Set e Gruppi** | `[abc]` / `[^abc]` | Uno qualsiasi tra `a,b,c` / Tutto **tranne** `a,b,c` |
| | `(abc)` / `(?:abc)` | Gruppo di cattura / Gruppo non catturante |
| | `a\|b` | `a` OPPURE `b` |

#### Come funzionano le parentesi quadre `[...]` (Character Set)
- **Sempre un singolo carattere**: `[a-z]` cattura 1 lettera; per una parola serve un quantificatore (`[a-z]+`).
- **Elenco**: `/c[ao]sa/g` matcha `"casa"` e `"cosa"`.
- **Intervalli (ranges)**: `[a-z]`, `[A-Z]`, `[0-9]`, `[a-zA-Z0-9_]`.
- **Negazione con `^` all'inizio**: `/[^0-9]+/g` su `"123abc45"` $\to$ `["abc"]`. *(Se `^` non è in prima posizione, es. `[a^b]`, cerca il carattere letterale `^`.)*
- **Caratteri speciali disattivati**: dentro `[.?!]` i simboli `.`, `?`, `!`, `+`, `*` sono cercati letteralmente. Unici caratteri speciali dentro `[...]`: `]`, `\`, `^` (all'inizio) e `-` (mettilo alla fine per un trattino letterale: `[a-z-]`).

#### I 4 metodi Regex & comportamento di `.match()`
```js
const testo = "Ordine #1042 confermato, totale: 85 euro";

/\d+/.test(testo);                    // true  (validazione booleana)
testo.match(/\d+/g);                  // ["1042", "85"]  (CON /g -> array di tutti i match)
testo.replace(/\d+/g, "XXX");         // "Ordine #XXX confermato, totale: XXX euro"
"mela, pera; banana".split(/[,;]\s*/); // ["mela", "pera", "banana"]
```
- **`.match()` CON `/g`**: restituisce un array piatto di tutte le corrispondenze (`["2", "15"]`), oppure `null`.
- **`.match()` SENZA `/g`**: si ferma al **primo** match e restituisce anche i **gruppi di cattura** `(...)`:
  ```js
  const res = "Data: 2026-11-19".match(/(\d{4})-(\d{2})-(\d{2})/);
  // res[0] = "2026-11-19", res[1] = "2026", res[2] = "11", res[3] = "19", res.index = 6
  ```
- **`.matchAll(/.../g)`**: richiede `/g` e restituisce un iteratore con **tutti** i match **e** i rispettivi gruppi di cattura (`[...str.matchAll(/.../g)]`).

---

### 13.5 Parsing di Numeri e JSON

#### Da Stringa a Numero
| Metodo | Comportamento | Esempio |
| :--- | :--- | :--- |
| `Number(str)` | Converte l'intera stringa; testo extra $\to$ `NaN` | `Number("42")` $\to$ `42` \| `Number("42px")` $\to$ `NaN` |
| `parseInt(str, 10)` | Legge un intero dall'inizio e ignora il resto | `parseInt("42px", 10)` $\to$ `42` |
| `parseFloat(str)` | Legge un decimale dall'inizio (separatore `.`) | `parseFloat("3.14m")` $\to$ `3.14` |

> Verifica sempre il fallimento con **`Number.isNaN(val)`** (mai `val === NaN`, che è sempre `false`).

#### Parsing JSON (sempre protetto con `try...catch`)
```js
try {
    const dati = JSON.parse(jsonStr);
    // uso dati...
} catch (err) {
    node.error("JSON non valido: " + err.message, msg);
    return null;
}
```

---

### 13.6 Pattern Classici da Esame & Trappole JS

#### Pattern pronti
```js
// A) Riga CSV -> campi puliti + numero
const [nome, cognome, etaStr] = "  Mario , Rossi , 25  ".split(",").map(s => s.trim());
const eta = Number(etaStr);

// B) Coppie chiave=valore (es. "user=anna&role=admin&active=true")
const obj = {};
for (const coppia of query.split("&")) {
    const [k, v] = coppia.split("=");
    obj[k] = v;
}
```

#### Trappole d'Esame (Errori Frequenti)
1. **Dimenticare `|| []` dopo `.match()`**: se non c'è match restituisce `null` $\to$ `.length` / `.includes()` / `.map()` lanciano `TypeError`.
2. **Usare `for (let tok in arr)` al posto di `for (let tok of arr)`**: `in` itera sugli **indici** (`"0"`, `"1"`, ...), **non** sui token!
3. **Chiamare `.trim()` pensando di ottenere un array**: `.trim()` restituisce una stringa; serve `.split(/\s+/)` o `.match(...)`.
4. **Usare `.split(" ")` su spazi multipli**: genera stringhe vuote `""` nell'array. Usa `.trim().split(/\s+/)`.
5. **`Number("")` e `Number("   ")` restituiscono `0`**, non `NaN`! Controlla sempre `str.trim() !== ""` prima di convertire.
6. **Dimenticare la base `10` in `parseInt(str, 10)`**: evita interpretazioni esadecimali inattese su prefissi `0x`.
7. **Non proteggere `JSON.parse()` con `try...catch`**: un payload corrotto blocca l'esecuzione con `SyntaxError`.
8. **Input non stringa da Telegram** (sticker, foto, audio): controlla sempre `typeof msg.payload?.content === "string"` prima di chiamare metodi di stringa.

---

## 14. Case Study d'Esame 2 — Telegram Weather Tracker Bot (`QUERY` / `TRACK` / `REPORT`)

*(Rif: Evaluation Lab `assignment.pdf` — flow pulito di riferimento in [`nodered-practice/exam2025-clean-export.json`](../nodered-practice/exam2025-clean-export.json) e tab `Exam2025` di [`nodered-practice/flows.json`](../nodered-practice/flows.json))*

### 14.1 Requisiti Tipici della Traccia
- Interfaccia Telegram verso **OpenWeatherMap** (`wtype: "current"`) per **qualsiasi località** di `openweathermap.org/find` (non più solo città fisse!).
- Tre comandi:
  - `QUERY <location> <country>` $\to$ `"The temperature in Milan is 3C!"`
  - `TRACK <location> <country>` $\to$ aggiunge la località alle preferenze **di quello specifico utente** e risponde `"Tracking temperature for Milan!"`
  - `REPORT` $\to$ invia la temperatura attuale di **tutte** le località salvate dall'utente corrente (un messaggio per città).
- **Multi-utente obbligatorio**: utenti diversi hanno liste indipendenti.
- **Vincolo RedBot sul nodo `Text` (`chatbot-message`)**: può usare Handlebars **solo su `msg.payload`** (es. `{{payload.message}}` o `{{payload.tempc}}` / `{{payload.location}}`), **non** su proprietà custom come `{{location.city}}`.

---

### 14.2 Architettura Pulita (6 Nodi Essenziali)
```text
Telegram In ─▶ Filter (2 uscite) ─┬─[Out 1: QUERY / REPORT]─▶ Temperature OW (dinamico) ─▶ MessageBuilder ─┐
                                  │                                                                        ▼
                                  └─[Out 2: TRACK / Error]─────────────────────────────────────────▶ ReplyMessage ─▶ Telegram Out
```
- **Singolo nodo `Temperature OW`** (`wtype: "current"`, campi `city` e `country` **vuoti**): legge dinamicamente `msg.location = { city, country }`.
- **Riuso completo dello stesso ramo** sia per `QUERY` (1 messaggio) sia per `REPORT` (array di $K$ messaggi clonati).
- **Singolo nodo `ReplyMessage`** (`chatbot-message`) con template `{{payload.message}}`, alimentato sia da `MessageBuilder` sia da `Filter` (Out 2: conferma `TRACK` ed eventuali messaggi di fallback).
  - *(Variante ancora più corta)*: poiché `openweathermap` scrive il nome città anche in `msg.payload.location`, il ramo `QUERY`/`REPORT` può andare anche direttamente a un nodo `Text` dedicato con `The temperature in {{payload.location}} is {{payload.tempc}}C!`.

---

### 14.3 Pattern 1 — Parsing di Comandi con Città Multi-Parola (`New York US`, `Reggio Emilia IT`)
> ⚠️ **Trappola**: `raw.toLowerCase().match(/[a-z0-9]+/g)` con `tokens.length === 3` **fallisce** su città composte da 2+ parole (es. `QUERY New York US` produce 4 token!) e perde le maiuscole (`"milan"` invece di `"Milan"`).

**Pattern robusto**:
```js
const raw = typeof msg.payload?.content === "string" ? msg.payload.content.trim() : "";
const tokens = raw ? raw.split(/\s+/) : [];
const command = (tokens[0] || "").toLowerCase();

const isValidQueryOrTrack = tokens.length >= 3 && ["query", "track"].includes(command);
const isValidReport = tokens.length === 1 && command === "report";

if (!isValidQueryOrTrack && !isValidReport) {
    msg.payload.message = "I don't get what you're asking...";
    return [null, msg];
}

// Per QUERY / TRACK: il paese è l'ultimo token, la città è tutto ciò che sta in mezzo (preserva maiuscole!)
const queryCity = tokens.slice(1, -1).join(" ");
const queryCountry = tokens[tokens.length - 1];
```

---

### 14.4 Pattern 2 — Stato Multi-Utente in `context` (Senza Duplicati né Bug di Chiave)
```js
// Preferisci from.id di Telegram (distingue utenti anche in chat di gruppo)
const userId = String(msg.originalMessage?.from?.id ?? msg.payload?.userId ?? msg.payload?.chatId);

// Mappa unica sotto chiave stringa fissa "userPrefs" (evita bug di Node-RED con chiavi numeriche/negative)
const allPrefs = context.get("userPrefs") || {};
const userPrefs = allPrefs[userId] || [];

const alreadyTracked = userPrefs.some(
    p => p.city.toLowerCase() === queryCity.toLowerCase() &&
         p.country.toLowerCase() === queryCountry.toLowerCase()
);

if (!alreadyTracked) {
    userPrefs.push({ city: queryCity, country: queryCountry });
    allPrefs[userId] = userPrefs;       // aggancia l'array se è la prima città dell'utente
    context.set("userPrefs", allPrefs); // persiste la mappa aggiornata nel context
}

msg.payload.message = "Tracking temperature for " + queryCity + "!";
return [null, msg];
```
*(Se servisse anche persistenza tra riavvii portabile su qualsiasi PC senza modificare `settings.js`, usa `flow.get/set("userPrefs")` + nodo `file` (`Overwrite`) su `TRACK` e un piccolo flow `inject (once: true)` $\to$ `file in` $\to$ `function` che fa `JSON.parse` all'avvio.)*

---

### 14.5 Pattern 3 — `REPORT` Fan-Out (`RED.util.cloneMessage`) & `MessageBuilder`
In **`Filter`** (ramo `report`):
```js
if (command === "report") {
    const allPrefs = context.get("userPrefs") || {};
    const userPrefs = allPrefs[userId] || [];
    const userQueryMsg = [];

    for (const pref of userPrefs) {
        // Clona DENTRO il ciclo: preserva msg.originalMessage e msg.chat(), ma isola payload/location/_msgid
        const msgReport = RED.util.cloneMessage(msg);
        msgReport.location = {
            city: pref.city,
            country: pref.country
        };
        userQueryMsg.push(msgReport);
    }

    // Restituisce l'array di messaggi sulla porta 1 (verso Temperature OW)
    return [userQueryMsg, null];
}
```
In **`MessageBuilder`** (dopo `Temperature OW`):
```js
const city = msg.location?.city || msg.payload?.location || "";
const temperature = msg.payload.tempc;

msg.payload.message = "The temperature in " + city + " is " + temperature + "C!";
return msg;
```

### 14.6 Riepilogo Bug Tipici di Questo Pattern
| Sintomo | Causa Tipica | Fix |
| :--- | :--- | :--- |
| `REPORT` invia solo l'ultima città tracciata | Hai inviato `msg` (`node.send([msg, ...])`) o dichiarato `let msgReport = {}` **fuori** dal `for` | Crea `let msgReport = RED.util.cloneMessage(msg)` **dentro** il `for` e fai `return [userQueryMsg, null]` |
| `REPORT` non invia nulla su Telegram | Hai restituito `userPrefs` (dati `{city, country}`) invece di `userQueryMsg` (messaggi RedBot) | Restituisci sempre l'array dei messaggi clonati da `msg` |
| Due utenti si sovrascrivono a vicenda | Hai passato un `chatId`/`userId` **numerico** direttamente a `context.set(id, ...)` | Usa sempre una chiave fissa (`context.set("userPrefs", allPrefs)`) con `allPrefs[String(userId)]` |
| `QUERY New York US` rifiutata | Check rigido `tokens.length === 3` | Controlla `tokens.length >= 3`, paese = `tokens.at(-1)`, città = `tokens.slice(1, -1).join(" ")` |
| `"REPORT foo bar"` accettata per errore | `"report"` lasciato dentro `acceptedCommands` controllato con `tokens.length >= 3` | Separa i controlli: `["query", "track"]` con `>= 3`, `"report"` con `=== 1` |
