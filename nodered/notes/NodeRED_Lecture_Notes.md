# Node-RED — Fast Exam Reference & Code Playbook (NSDS)

Appunti completi e prontuario operativo basati sulle lezioni:
- [`1.1-Node-RED Basics.pdf`](../lectures/1.1-Node-RED%20Basics.pdf)
- [`1.2-Node-RED Processing Nodes.pdf`](../lectures/1.2-Node-RED%20Processing%20Nodes.pdf)
- [`1.3-Node-RED Data Sharing.pdf`](../lectures/1.3-Node-RED%20Data%20Sharing.pdf)
- [`2.1-Node-RED Networking UDP.pdf`](../lectures/2.1-Node-RED%20Networking%20UDP.pdf)
- [`2.2-Node-RED Networking MQTT.pdf`](../lectures/2.2-Node-RED%20Networking%20MQTT.pdf)

---

## 1. Matrice Rapida: Quale Pattern Usare?

| Esigenza / Scenario d'Esame | Meccanismo Node-RED | Template Pronto (`nodered-lab-template/flows/`) |
| :--- | :--- | :--- |
| Generare un evento manuale, periodico o **solo all'avvio** | `inject` (`repeat`, oppure `once: true` per boot) | [`01_basics_and_async.json`](../nodered-lab-template/flows/01_basics_and_async.json) |
| Instradare messaggi su rami diversi senza codice JS | `switch` (es. `payload > 15` vs `otherwise`) | [`01_basics_and_async.json`](../nodered-lab-template/flows/01_basics_and_async.json) |
| Trasformare, filtrare o elaborare `msg.payload` in JS | `function` (`On Start`, `On Message`, `On Stop`) | [`02_function_nodes_patterns.json`](../nodered-lab-template/flows/02_function_nodes_patterns.json) |
| Inviare messaggi su **più uscite distinte** da un `function` | Impostare `Outputs = N` e fare `return [msg, null, null]` | [`02_function_nodes_patterns.json`](../nodered-lab-template/flows/02_function_nodes_patterns.json) |
| Emettere **una sequenza di $K$ messaggi** su un'uscita | `return [msgList];` (array di array!) oppure `node.send([msgList]);` | [`02_function_nodes_patterns.json`](../nodered-lab-template/flows/02_function_nodes_patterns.json) |
| Condividere stato tra invocazioni **dello stesso nodo** | `context.get("k")` / `context.set("k", v)` | [`03_data_sharing_files_and_context.json`](../nodered-lab-template/flows/03_data_sharing_files_and_context.json) |
| Condividere stato tra nodi dello **stesso tab/flow** | `flow.get("k")` / `flow.set("k", v)` | [`03_data_sharing_files_and_context.json`](../nodered-lab-template/flows/03_data_sharing_files_and_context.json) |
| Condividere stato tra **tab/flow diversi** della stessa istanza | `global.get("k")` / `global.set("k", v)` | [`03_data_sharing_files_and_context.json`](../nodered-lab-template/flows/03_data_sharing_files_and_context.json) |
| Mantenere lo stato tra **deploy e riavvii** | Nodi `file` / `file in` **oppure** `contextStorage` su `localfilesystem` (`context.get("k", "file")`) | [`03_data_sharing_files_and_context.json`](../nodered-lab-template/flows/03_data_sharing_files_and_context.json) |
| Inviare/ricevere oggetti JS via **UDP** (`127.0.0.1:port`) | `json` (`Object -> JSON string`) $\to$ `udp out` / `udp in` $\to$ `json` (`JSON string -> Object`) | [`04_udp_json_networking.json`](../nodered-lab-template/flows/04_udp_json_networking.json) |
| Pub/Sub disaccoppiato su topic gerarchici (**MQTT**) | `mqtt out` / `mqtt in` verso `localhost:1883` (`QoS 0/1/2`, auto-JSON) | [`05_mqtt_pubsub_and_qos.json`](../nodered-lab-template/flows/05_mqtt_pubsub_and_qos.json) |

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
2. **Volatilità su Deploy / Riavvio**:
   - Con lo store di default (`memory`), il context viene **azzerato al riavvio di Node-RED** (e anche su *Full Deploy*; con *Modified Nodes* si resetta solo per i nodi modificati).
   - Per renderlo **persistente su disco**, in `settings.js` si configura `contextStorage` con `"localfilesystem"` (già configurato in `nodered-exam/settings.js` e `nodered-lab-template/settings.js`!):
     ```js
     // Legge/scrive sullo store persistente chiamato "file"
     const count = context.get("count", "file") || 0;
     context.set("count", count + 1, "file");
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
