# Apache Kafka — Cheatsheet Definitivo per Esame (Garanzie, Configurazioni, Lettura & Invio)

Guida centralizzata e operativa basata sui sorgenti della repository (`base/`, `lab1/`, `lab2/`, `exam-2023/`, `exam-2024/`, `kafka-lab-template/`) e sulle slide del corso (`kafka_02_overview.pdf`, `kafka_03_guarantees.pdf`), pensata per consultazione immediata in sede d'esame.

> **Nota critica sulla versione nel progetto (`kafka-clients` 3.5.1)**  
> In Kafka **3.5.1** i default del `KafkaProducer` sono cambiati rispetto alle versioni vecchie (< 3.0):
> - `ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG` = **`"true"`** (default!)
> - `ProducerConfig.ACKS_CONFIG` = **`"all"`** (default!)
> - `ProducerConfig.RETRIES_CONFIG` = **`Integer.MAX_VALUE`** (default!)
>
> **Implicazione pratica d'esame**:
> 1. Se vuoi disattivare l'idempotenza sul Producer (per ottenere **None**, **At-Most-Once** o **At-Least-Once** puro), devi impostare **esplicitamente** `ENABLE_IDEMPOTENCE_CONFIG = "false"`.
> 2. Se lasci `ENABLE_IDEMPOTENCE_CONFIG = "true"` (o lo ometti) ma imposti `ACKS_CONFIG = "0"` o `RETRIES_CONFIG = "0"`, Kafka 3.5.1 lancia una **`ConfigException`** (se `enable.idempotence` è stato impostato dall'utente) oppure disabilita l'idempotenza con un warning! Imposta sempre `ENABLE_IDEMPOTENCE_CONFIG` in modo esplicito.
> 3. Sul `KafkaConsumer`, invece, i default restano pericolosi per garanzie forti:
>    - `ENABLE_AUTO_COMMIT_CONFIG` = **`"true"`** (default)
>    - `AUTO_COMMIT_INTERVAL_MS_CONFIG` = **`"5000"`** (default)
>    - `AUTO_OFFSET_RESET_CONFIG` = **`"latest"`** (default)
>    - `ISOLATION_LEVEL_CONFIG` = **`"read_uncommitted"`** (default!)

---

## 1. Le 4 Garanzie di Consegna: Definizioni e Cosa Succede in Caso di Failure

| Garanzia | Significato (Teoria & Esame) | Duplicati? | Perdite? | Quando usarla / Esempi nella Repo |
| :--- | :--- | :---: | :---: | :--- |
| **None** *(Best-Effort / Nessuna garanzia)* | Nessuna protezione in caso di crash o errori di rete. | **Sì** | **Sì** | Traccia: *"does NOT provide guarantees in the case of failures"* (`PopularTopicConsumer` Exam 2023, `Consumer2` Exam 2024, `lab1/ex1-ex2`). |
| **At-Most-Once** *(0 o 1 volta)* | Ogni messaggio è elaborato/consegnato **al massimo una volta**. Può essere perso **solo** in caso di failure. | **NO** | **Sì** *(solo su crash/errore)* | Traccia: *"0 or 1 times, messages may be missed only in the case of a failure"* (`AtMostOncePrinter` Exam 2023). |
| **At-Least-Once** *(1 o più volte)* | Ogni messaggio è elaborato/consegnato **almeno una volta**. Mai perso, ma un crash può causare rielaborazioni. | **Sì** *(su crash)* | **NO** | Traccia: *"each input message is evaluated at least once"* (`Merger` Lab 2, `BasicConsumerManual`). |
| **Exactly-Once (EOS)** *(1 e 1 sola volta)* | Il sistema si comporta come se ogni messaggio fosse elaborato/consegnato **una e una sola volta**, anche in presenza di crash. | **NO** | **NO** | Traccia: *"exactly once guarantees / atomicity"* (`AtomicUpperCaseConsumer`, `AtomicFaultTolerantForwarder`, `Consumer1` Exam 2024, `Validator` Lab 2). |

> **Regola teorica fondamentale (`kafka_03_guarantees.pdf`)**:
> - Tra **Producer $\to$ Partition**: EOS si ottiene con `enable.idempotence = true`.
> - Tra **Topic Input $\to$ Elaborazione $\to$ Topic Output** (*Forwarder*): EOS si ottiene con **Transazioni** (`transactional.id` + `sendOffsetsToTransaction` + `read_committed` + eventuale stato su topic compattato).
> - Un **Consumer puro** che scrive solo su `stdout` o DB esterno *non* può avere EOS garantito da Kafka da solo (può avere solo **None**, **At-Most-Once**, oppure **At-Least-Once** + deduplica applicativa idempotente lato sink).

---

## 2. Matrice Rapida da Esame (`Ruolo × Garanzia`)

### 2.1 Producer (`ProducerConfig`)

| Garanzia | `ENABLE_IDEMPOTENCE_CONFIG` | `ACKS_CONFIG` | `RETRIES_CONFIG` | `TRANSACTIONAL_ID_CONFIG` | Modalità di invio & Note | Template / Rif. Repo |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **None** | `"false"` | `"0"` | `"0"` | `null` | `producer.send(record)` asincrono fire-and-forget, senza `.get()` né `flush()`. | [`BasicProducerTemplate`](../kafka-lab-template/src/main/java/com/polimi/kafka/template/BasicProducerTemplate.java) (`none`) |
| **At-Most-Once** | `"false"` | `"1"` *(o `"0"`)* | **`"0"`** | `null` | Nessun retry: tenta 1 volta sola. Se l'ACK si perde o il broker fallisce, non ritrasmette $\implies$ mai duplicati. | [`BasicProducerTemplate`](../kafka-lab-template/src/main/java/com/polimi/kafka/template/BasicProducerTemplate.java) (`at-most-once`) |
| **At-Least-Once** | `"false"` | `"all"` *(o `"1"`)* | `MAX_VALUE` *(> 0)* | `null` | `producer.send(record).get()` o `flush()`. Se l'ACK si perde dopo scrittura, ritrasmette $\implies$ possibili duplicati, zero perdite. | [`BasicProducerTemplate`](../kafka-lab-template/src/main/java/com/polimi/kafka/template/BasicProducerTemplate.java) (`at-least-once`) |
| **Exactly-Once (Singola Partizione)** | **`"true"`** | `"all"` *(obbl.)* | `> 0` *(obbl.)* | `null` | Broker assegna `PID` + `sequence number` e scarta duplicati dovuti ai retry. | [`IdempotentProducer`](../base/src/main/java/it/polimi/middleware/kafka/atomic_forward/IdempotentProducer.java), [`Producer` (Exam 23)](../exam-2023/NSDS_kafka_eval/src/main/java/it/polimi/nsds/kafka/Producer.java) |
| **Exactly-Once (Atomico Multi-Record)** | **`"true"`** *(obbl.)* | `"all"` *(obbl.)* | `> 0` *(obbl.)* | **`"txId"`** *(univoco)* | `initTransactions()` $\to$ `beginTransaction()` $\to$ `send()` $\to$ `commitTransaction()` / `abortTransaction()`. | [`TransactionalProducerTemplate`](../kafka-lab-template/src/main/java/com/polimi/kafka/template/TransactionalProducerTemplate.java) |

---

### 2.2 Consumer (`ConsumerConfig`)

| Garanzia | `ENABLE_AUTO_COMMIT_CONFIG` | `ISOLATION_LEVEL_CONFIG` | Ordine Operazioni nel Loop `poll()` | Comportamento al Crash | Template / Rif. Repo |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **None** | **`"true"`** | `"read_uncommitted"` *(o `"read_committed"`)* | `poll()` $\to$ elabora (l'auto-commit gira ogni `auto.commit.interval.ms`). | Crash prima dell'auto-commit $\implies$ duplicati. Crash dopo auto-commit di un batch parzialmente elaborato $\implies$ perdite. | [`BasicConsumerTemplate`](../kafka-lab-template/src/main/java/com/polimi/kafka/template/BasicConsumerTemplate.java), [`BasicConsumer`](../base/src/main/java/it/polimi/middleware/kafka/basic/BasicConsumer.java) |
| **At-Most-Once** | **`"false"`** | `"read_committed"` *(consigliato)* | **PRIMA `commitSync(...)`**, **POI** elabora/stampa il record! | Se crasha dopo `commitSync()` ma prima di elaborare, quel record (o il resto del batch) è perso, ma **mai duplicato**. | [`AtMostOnceConsumerTemplate`](../kafka-lab-template/src/main/java/com/polimi/kafka/template/AtMostOnceConsumerTemplate.java), [`AtMostOncePrinter`](../exam-2023/NSDS_kafka_eval/src/main/java/it/polimi/nsds/kafka/AtMostOncePrinter.java) |
| **At-Least-Once** | **`"false"`** | `"read_committed"` *(consigliato)* | **PRIMA** elabora il record/batch, **POI `commitSync(...)`**! | Se crasha prima di `commitSync()`, al riavvio rilegge e rielabora gli stessi record (**duplicati**, ma **mai perdite**). | [`ManualCommitConsumerTemplate`](../kafka-lab-template/src/main/java/com/polimi/kafka/template/ManualCommitConsumerTemplate.java), [`BasicConsumerManual`](../base/src/main/java/it/polimi/middleware/kafka/basic/BasicConsumerManual.java) |
| **Lettura Transazionale** | `"false"` *(o `"true"`)* | **`"read_committed"`** | Scarta i messaggi di transazioni aperte o abortite dal producer. | Evita di leggere dati sporchi/abortiti (`read_uncommitted` legge tutto, anche abortiti). | [`TransactionalConsumerTemplate`](../kafka-lab-template/src/main/java/com/polimi/kafka/template/TransactionalConsumerTemplate.java) |

---

### 2.3 Forwarder (`Read -> Process -> Write` da `inputTopic` a `outputTopic`)

| Garanzia | Consumer Config | Producer Config | Ordine Critico nel Codice | Gestione Stato / Errori | Template / Rif. Repo |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **None** | `auto.commit = "true"` | `idempotence = "false"`, `acks = "0"` | `poll()` $\to$ elabora $\to$ `producer.send()` (no `flush`, no `commitSync`). | Stato in RAM perso su crash (`Consumer2` Exam 24). | [`BasicForwarderTemplate`](../kafka-lab-template/src/main/java/com/polimi/kafka/template/BasicForwarderTemplate.java), [`UpperCaseConsumer`](../lab/lab1/src/main/java/it/polimi/middleware/kafka/ex2/UpperCaseConsumer.java) |
| **At-Most-Once** | `auto.commit = "false"`, `read_committed` | `idempotence = "false"`, `retries = "0"`, `acks = "1"` | **PRIMA `consumer.commitSync(...)`**, **POI** elabora e `producer.send()`. | Crash tra commit e send/ack $\implies$ messaggio perso, **mai duplicato**. | [`AtMostOnceForwarderTemplate`](../kafka-lab-template/src/main/java/com/polimi/kafka/template/AtMostOnceForwarderTemplate.java) |
| **At-Least-Once** *(Stateless / Stateful)* | `auto.commit = "false"`, `read_committed` | `idempotence = "false"` *(o `"true"`)*, `acks = "all"`, `retries = MAX` | Elabora + `producer.send()` $\to$ **`producer.flush()`** $\to$ **`consumer.commitSync()`**. | Se stateful: `ConsumerRebalanceListener` con replay `[0, committed)` senza re-emettere output (`Merger` Lab 2). | [`AtLeastOnceForwarderTemplate`](../kafka-lab-template/src/main/java/com/polimi/kafka/template/AtLeastOnceForwarderTemplate.java), [`Merger` (Lab 2)](../lab/lab2/NSDS26_kafka_lab2/src/main/java/it/polimi/middleware/kafka/eval/ConsumersXX.java) |
| **Exactly-Once Stateless** | `auto.commit = "false"`, `read_committed` | `idempotence = "true"`, `transactional.id = <univoco>` | `beginTransaction()` $\to$ `send()` $\to$ `sendOffsetsToTransaction(lastOffset + 1)` $\to$ `commitTransaction()`. | `ProducerFencedException` $\to$ `close()`; `KafkaException` $\to$ `abortTransaction()` + `consumer.seek()`. | [`TransactionalForwarderTemplate`](../kafka-lab-template/src/main/java/com/polimi/kafka/template/TransactionalForwarderTemplate.java), [`AtomicUpperCaseConsumer`](../lab/lab1/src/main/java/it/polimi/middleware/kafka/ex3/AtomicUpperCaseConsumer.java), [`Validator` (Lab 2)](../lab/lab2/NSDS26_kafka_lab2/src/main/java/it/polimi/middleware/kafka/eval/ConsumersXX.java) |
| **Exactly-Once Windowed** | `auto.commit = "false"`, `read_committed` | `idempotence = "true"`, `transactional.id = <univoco>` | `window` **fuori** dal `while(true)`. Transazione aperta **solo** quando `window.size() == N`. | Committa solo gli offset dei record nella finestra emessa, mai dei record parziali! | [`WindowedForwarderTemplate`](../kafka-lab-template/src/main/java/com/polimi/kafka/template/WindowedForwarderTemplate.java), [`Consumer1` (Exam 24)](../exam-2024/NSDS24_kafka_eval_solutions/src/main/java/it/polimi/nsds/kafka/eval/Consumers.java) |
| **Exactly-Once Stateful** | `auto.commit = "false"`, `read_committed` | `idempotence = "true"`, `transactional.id = <univoco>` | `storeConsumer` (`assign` + `seekToBeginning` su `outputTopic` compattato) all'avvio **e** in `onPartitionsAssigned()`. | Usa `pendingUpdates` e aggiorna la `Map` in RAM **solo dopo** `commitTransaction()`. | [`StatefulForwarderTemplate`](../kafka-lab-template/src/main/java/com/polimi/kafka/template/StatefulForwarderTemplate.java), [`AtomicFaultTolerantForwarder`](../lab/lab1/src/main/java/it/polimi/middleware/kafka/ex4/AtomicFaultTolerantForwarder.java) |

---

## 3. Configurazioni Dettagliate del PRODUCER (`ProducerConfig`)

### 3.1 Proprietà Principali

| Costante `ProducerConfig.*` | Chiave Stringa | Valori Tipici | Default (Kafka 3.5.1) | Significato |
| :--- | :--- | :--- | :--- | :--- |
| `BOOTSTRAP_SERVERS_CONFIG` | `"bootstrap.servers"` | `"localhost:9092"` | Obbligatorio | Broker iniziali del cluster. |
| `KEY_SERIALIZER_CLASS_CONFIG` | `"key.serializer"` | `StringSerializer.class.getName()` | Obbligatorio | Serializza la chiave `K` in `byte[]`. |
| `VALUE_SERIALIZER_CLASS_CONFIG` | `"value.serializer"` | `StringSerializer.class.getName()` / `IntegerSerializer.class.getName()` | Obbligatorio | Serializza il valore `V` in `byte[]`. |
| `ENABLE_IDEMPOTENCE_CONFIG` | `"enable.idempotence"` | `"true"` / `"false"` | `"true"` *(in 3.5.1!)* | Se `"true"`, il broker usa `PID` + `sequence number` per scartare duplicati da retry sulla singola partizione. |
| `ACKS_CONFIG` | `"acks"` | `"0"`, `"1"`, `"all"` (`"-1"`) | `"all"` | `"0"` = nessun ACK; `"1"` = solo leader; `"all"` = leader + tutte le repliche ISR. |
| `RETRIES_CONFIG` | `"retries"` | `"0"`, `String.valueOf(Integer.MAX_VALUE)` | `Integer.MAX_VALUE` | Numero di ritrasmissioni automatiche su errori recuperabili/timeout. |
| `MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION` | `"max.in.flight.requests.per.connection"` | `"1"` .. `"5"` | `"5"` | Con idempotenza deve essere $\le 5$ per preservare l'ordine FIFO. |
| `TRANSACTIONAL_ID_CONFIG` | `"transactional.id"` | `"txProducer-1"` | `null` | Abilita le transazioni atomiche e lo *zombie fencing* sul Transaction Coordinator. |

### 3.2 Snippet per Ciascuna Garanzia sul Producer

#### A) Producer **NONE** (Fire-and-Forget)
```java
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(false)); // esplicito in 3.5.1!
props.put(ProducerConfig.ACKS_CONFIG, "0");
props.put(ProducerConfig.RETRIES_CONFIG, "0");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.send(new ProducerRecord<>("inputTopic", key, value)); // nessuna attesa ACK
```

#### B) Producer **AT-MOST-ONCE** (Zero Retry: 0 o 1 volta)
```java
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(false));
props.put(ProducerConfig.ACKS_CONFIG, "1");     // oppure "0"
props.put(ProducerConfig.RETRIES_CONFIG, "0");  // CRITICO: nessun retry automatico -> mai duplicati!

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
try {
    producer.send(new ProducerRecord<>("inputTopic", key, value)).get(); // 1 solo tentativo
} catch (InterruptedException | ExecutionException e) {
    // Non ritrasmettere! Se l'ACK si è perso dopo la scrittura, ritrasmettere creerebbe un duplicato.
}
```

#### C) Producer **AT-LEAST-ONCE** (Retry attivi senza idempotenza)
```java
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(false)); // disabilita deduplica broker
props.put(ProducerConfig.ACKS_CONFIG, "all");
props.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
RecordMetadata ack = producer.send(new ProducerRecord<>("inputTopic", key, value)).get(); // oppure flush()
```

#### D) Producer **EXACTLY-ONCE** — Idempotente (Singola Partizione) vs Transazionale (Multi-Messaggio)
- **Idempotente** (ogni `send` individuale è scritto esattamente 1 volta sulla sua partizione):
```java
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));
props.put(ProducerConfig.ACKS_CONFIG, "all");
```
- **Transazionale** (più `send` su più partizioni/topic in modalità tutto-o-niente):
```java
props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "myTxId"); // UNIVOCO per istanza!
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.initTransactions(); // 1 sola volta prima del loop!

producer.beginTransaction();
try {
    producer.send(new ProducerRecord<>("topic1", k1, v1));
    producer.send(new ProducerRecord<>("topic2", k2, v2));
    producer.commitTransaction();
} catch (ProducerFencedException | OutOfOrderSequenceException e) {
    producer.close(); // Errore fatale: un'altra istanza ha lo stesso transactional.id (Zombie Fencing)
} catch (KafkaException e) {
    producer.abortTransaction(); // Annulla tutta la transazione
}
```
> **Perché in `TransactionalProducer.java` si chiama `producer.flush()` prima di `producer.abortTransaction()`?**  
> Perché `send()` mette i record nel buffer RAM locale del producer. Senza `flush()`, `abortTransaction()` svuota solo il buffer locale senza scrivere nulla sul broker. Con `flush()`, i record arrivano fisicamente sul log seguiti dal marker di *abort*: così puoi testare che un consumer `"read_uncommitted"` li legge lo stesso, mentre `"read_committed"` li scarta!

---

## 4. Configurazioni Dettagliate del CONSUMER (`ConsumerConfig`)

### 4.1 Proprietà Principali

| Costante `ConsumerConfig.*` | Chiave Stringa | Valori Tipici | Default | Significato |
| :--- | :--- | :--- | :--- | :--- |
| `BOOTSTRAP_SERVERS_CONFIG` | `"bootstrap.servers"` | `"localhost:9092"` | Obbligatorio | Broker iniziali del cluster. |
| `GROUP_ID_CONFIG` | `"group.id"` | `"groupA"` | Obbligatorio *(con `subscribe`)* | Stesso `group.id` $\implies$ **Load Balancing** (1 partition = 1 consumer). Diverso `group.id` $\implies$ **Broadcast / Pub-Sub**. |
| `KEY_DESERIALIZER_CLASS_CONFIG` | `"key.deserializer"` | `StringDeserializer.class.getName()` | Obbligatorio | Deserializza la chiave. |
| `VALUE_DESERIALIZER_CLASS_CONFIG` | `"value.deserializer"` | `StringDeserializer.class.getName()` / `IntegerDeserializer.class.getName()` | Obbligatorio | Deserializza il valore. |
| `ENABLE_AUTO_COMMIT_CONFIG` | `"enable.auto.commit"` | `"true"` / `"false"` | `"true"` | `"true"` = commit automatico in background; `"false"` = commit manuale o transazionale. |
| `AUTO_COMMIT_INTERVAL_MS_CONFIG` | `"auto.commit.interval.ms"` | `"5000"`, `"15000"` | `"5000"` | Intervallo di auto-commit (usato solo se `enable.auto.commit = "true"`). |
| `AUTO_OFFSET_RESET_CONFIG` | `"auto.offset.reset"` | `"latest"`, `"earliest"` | `"latest"` | Usato **SOLO** se il `group.id` non ha ancora offset committati in `__consumer_offsets`! |
| `ISOLATION_LEVEL_CONFIG` | `"isolation.level"` | `"read_committed"`, `"read_uncommitted"` | `"read_uncommitted"` | `"read_committed"` scarta i messaggi di transazioni non ancora committate o abortite. |

### 4.2 Snippet per Ciascuna Garanzia sul Consumer

#### A) Consumer **NONE** (Auto-Commit — Rif. `BasicConsumer`, `PopularTopicConsumer` Exam 2023)
```java
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(true));
props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000"); // o "15000"
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");     // o "earliest"
```
- **Perché è None?** Se crashi prima del timer di 5s $\implies$ rileggi i record già stampati (**duplicati**). Se l'auto-commit scatta appena dopo `poll()` mentre il ciclo `for` non ha finito (o se perdi stato in RAM come `Map<String, Integer>`) $\implies$ dati persi/incoerenti (**perdite**).

#### B) Consumer **AT-MOST-ONCE** (Commit PRIMA di elaborare — Rif. `AtMostOncePrinter` Exam 2023)
```java
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
```
Hai **2 varianti** (entrambe in [`AtMostOnceConsumerTemplate.java`](../kafka-lab-template/src/main/java/com/polimi/kafka/template/AtMostOnceConsumerTemplate.java)):
1. **Per-record (consigliata in esame — perdita massima = 1 messaggio)**:

   ```java
   for (ConsumerRecord<String, Integer> record : records) {
       TopicPartition tp = new TopicPartition(record.topic(), record.partition());
       consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(record.offset() + 1)));
       if (record.value() > threshold) {
           System.out.println(record.key() + ": " + record.value());
       }
   }
   ```

2. **Per-batch (più veloce, ma se crashi a metà batch perdi tutti i successivi del batch)**:

   ```java
   if (!records.isEmpty()) {
       consumer.commitSync(); // committa tutti gli offset del poll() PRIMA del for!
       for (ConsumerRecord<String, Integer> record : records) {
           process(record);
       }
   }
   ```

#### C) Consumer **AT-LEAST-ONCE** (Commit DOPO aver elaborato — Rif. `BasicConsumerManual`)
```java
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
```
```java
for (ConsumerRecord<String, String> record : records) {
    process(record); // PRIMA elabora
}
if (!records.isEmpty()) {
    consumer.commitSync(); // POI committa l'intero batch
}
```
> **TRAPPOLA CLASSICISSIMA SU `commitSync()`**:  
> Chiamare `consumer.commitSync()` **senza argomenti** committa gli offset **dell'intero ultimo `poll()`**, NON fino al record corrente del `for`!  
> Se vuoi committare a metà batch (es. ogni singolo record o a chiusura di una finestra), devi passare la mappa esplicita `consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(record.offset() + 1)))`!

---

## 5. Configurazioni Dettagliate del FORWARDER (Read–Process–Write)

### 5.1 Forwarder **NONE** (`BasicForwarderTemplate`, `lab1/ex1-ex2`, `exam-2024/Consumer2`)
- **Consumer**: `ENABLE_AUTO_COMMIT_CONFIG = "true"`.
- **Producer**: `ENABLE_IDEMPOTENCE_CONFIG = "false"`, `ACKS_CONFIG = "0"` (o async senza `flush()`).
- **Flusso**: `poll()` $\to$ `producer.send(outRecord)` (senza `flush()` né `commitSync()`).

### 5.2 Forwarder **AT-MOST-ONCE** (`AtMostOnceForwarderTemplate`)
- **Consumer**: `ENABLE_AUTO_COMMIT_CONFIG = "false"`, `ISOLATION_LEVEL_CONFIG = "read_committed"`.
- **Producer**: `ENABLE_IDEMPOTENCE_CONFIG = "false"`, `RETRIES_CONFIG = "0"`, `ACKS_CONFIG = "1"` (o `"0"`).
- **Flusso**: `poll()` $\to$ **`consumer.commitSync(singleOffset)`** *(PRIMA)* $\to$ elabora $\to$ `producer.send(outRecord)` *(POI)* $\to$ `producer.flush()` opzionale.
- **Perché funziona**: una volta committato `offset + 1`, un riavvio dopo crash non rileggerà mai più quel messaggio di input; inoltre `retries = "0"` impedisce duplicati in scrittura verso `outputTopic`.

### 5.3 Forwarder **AT-LEAST-ONCE** (`AtLeastOnceForwarderTemplate`, `lab2/ConsumersXX.java` `Merger`)
- **Consumer**: `ENABLE_AUTO_COMMIT_CONFIG = "false"`, `ISOLATION_LEVEL_CONFIG = "read_committed"`, `AUTO_OFFSET_RESET_CONFIG = "earliest"`.
- **Producer**: `ENABLE_IDEMPOTENCE_CONFIG = "false"` *(o `"true"` per evitare duplicati da retry di rete)*, `ACKS_CONFIG = "all"`, `RETRIES_CONFIG = Integer.MAX_VALUE`.
- **Ordine obbligatorio nel loop**:
  ```java
  for (ConsumerRecord<String, Integer> record : records) {
      // aggiorna eventuale stato e invia output
      producer.send(new ProducerRecord<>(outputTopic, key, result));
  }
  producer.flush();      // 1. PRIMA garantisci che il broker abbia scritto tutti gli output!
  consumer.commitSync(); // 2. SOLO DOPO avanza gli offset di input!
  ```
- **Se il Forwarder At-Least-Once è STATEFUL (Pattern `Merger` di Lab 2)**:
  - Non puoi ripartire da offset `0` ri-emettendo tutti i vecchi output (troppe duplicazioni).
  - Usa un `ConsumerRebalanceListener`: dentro `onPartitionsAssigned(partitions)`, interroga `mainConsumer.committed(partitions)` per sapere fino a quale offset erano già stati committati i messaggi, e usa un **`recoveryConsumer` senza `group.id`** (`assign` + `seekToBeginning`) per rileggere l'intervallo `[0, committedOffset)` e ripopolare le mappe in RAM **senza chiamare `producer.send(...)`**, fermandoti prima di `committedOffset`!
  - Così, dopo crash/rebalance, lo stato in RAM è ricostruito perfettamente e verranno rielaborati/duplicati solo gli eventuali pochissimi messaggi non ancora committati dell'ultimo batch.

### 5.4 Forwarder **EXACTLY-ONCE (EOS) — Stateless** (`TransactionalForwarderTemplate`, `AtomicUpperCaseConsumer`, `lab2/Validator`)
- **Consumer**:
  - `ENABLE_AUTO_COMMIT_CONFIG = "false"` *(NON chiamare MAI `consumer.commitSync()`!)*
  - `ISOLATION_LEVEL_CONFIG = "read_committed"`
  - `AUTO_OFFSET_RESET_CONFIG = "earliest"`
- **Producer**:
  - `TRANSACTIONAL_ID_CONFIG = transactionalId` *(**UNIVOCO** per ogni istanza! Passarlo via `args`)*
  - `ENABLE_IDEMPOTENCE_CONFIG = "true"`
- **Ciclo atomico completo con gestione errori e `seek`**:
  ```java
  producer.initTransactions(); // 1 volta fuori dal while

  while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(5));
      if (records.isEmpty()) continue;

      try {
          producer.beginTransaction();

          for (ConsumerRecord<String, String> record : records) {
              producer.send(new ProducerRecord<>(outputTopic, record.key(), transform(record.value())));
          }

          // Calcola (lastOffset + 1) per OGNI partizione letta in questo poll()
          Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
          for (TopicPartition partition : records.partitions()) {
              List<ConsumerRecord<String, String>> pRecords = records.records(partition);
              long lastOffset = pRecords.get(pRecords.size() - 1).offset();
              offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
          }

          producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
          producer.commitTransaction();
      } catch (ProducerFencedException | OutOfOrderSequenceException e) {
          producer.close();
          consumer.close();
          break; // Fatale: zombie fencing o sequenza corrotta
      } catch (KafkaException e) {
          producer.abortTransaction();
          // Riavvolgi manualmente il cursore del consumer in RAM al 1° record del batch fallito!
          for (TopicPartition partition : records.partitions()) {
              if (consumer.assignment().contains(partition)) {
                  consumer.seek(partition, records.records(partition).get(0).offset());
              }
          }
      }
  }
  ```
- **Perché `lastOffset + 1`?** In Kafka l'offset committato indica il **prossimo** offset da leggere al riavvio.
- **Perché `consumer.seek(...)` su `KafkaException`?** `abortTransaction()` annulla la transazione sul broker, ma il cursore *in memoria* del `KafkaConsumer` era già avanzato durante il `poll()`. Senza `seek()`, il `poll()` successivo salterebbe il batch appena abortito!

### 5.5 Forwarder **EXACTLY-ONCE (EOS) — Tumbling Window** (`WindowedForwarderTemplate`, `exam-2024/Consumers.java`)
Tre errori mortali visti in `exam-2024/ConsumersXX.java` da evitare assolutamente:
1. **`window` VA DICHIARATA FUORI DAL `while (true)`**: `consumer.poll(Duration.ofMinutes(5))` **non** aspetta 5 minuti! Ritorna subito appena arriva almeno 1 messaggio. Se dichiari `counter`/`window` dentro il `while (true)`, si azzerano ad ogni `poll()`!
2. **`beginTransaction()` VA CHIAMATA SOLO QUANDO `window.size() == WINDOW_SIZE`**, non a inizio `poll()`.
3. **`sendOffsetsToTransaction(...)` DEVE COMMITTARE SOLO GLI OFFSET DELLA FINESTRA EMESSA**, MAI gli offset di tutti i record del `poll()` se alcuni restano parziali in attesa della finestra successiva!

### 5.6 Forwarder **EXACTLY-ONCE (EOS) — Stateful Fault-Tolerant** (`StatefulForwarderTemplate`, `lab1/ex4/AtomicFaultTolerantForwarder`)
Quando il forwarder mantiene uno stato in RAM (es. `Map<String, Integer> countMap`) e scrive il risultato su `outputTopic`:
1. **`outputTopic` con Log Compaction**: creato tramite `TopicAdminTemplate` (`TopicConfig.CLEANUP_POLICY_CONFIG = CLEANUP_POLICY_COMPACT`), così Kafka conserva almeno l'ultimo valore committato per ciascuna `key`.
2. **`storeConsumer` per recovery** (senza `group.id`, con `ISOLATION_LEVEL_CONFIG = "read_committed"`):
   - `partitionsFor(outputTopic)` $\to$ `assign(partitions)` $\to$ `seekToBeginning(partitions)` $\to$ `poll()` finché `isEmpty()`, popolando `countMap.put(r.key(), r.value())`.
3. **`ConsumerRebalanceListener` per multi-istanza**:
   - All'avvio **e** dentro `onPartitionsAssigned(partitions)`, richiamare `recoverStateFromChangelog(...)`! Così se un'istanza crasha e le sue partizioni passano a un'altra istanza del gruppo, questa ricarica gli ultimi valori aggiornati prima di processare nuovi messaggi.
4. **Rollback-safe `pendingUpdates`**:
   - Accumula le modifiche in `pendingUpdates` durante il ciclo e fai `countMap.putAll(pendingUpdates)` **solo dopo** che `producer.commitTransaction()` ha avuto successo!

---

## 6. Modalità per Inviare e Leggere i Messaggi (Riferimenti Codice Repo)

### 6.1 Tutte le Modalità di INVIO (`KafkaProducer`)

#### 1) Scelta della Partizione tramite `ProducerRecord`
| Costruttore `ProducerRecord` | Come viene scelta la partizione | Garanzia d'ordine & Località | Riferimento Repo |
| :--- | :--- | :--- | :--- |
| `new ProducerRecord<>(topic, key, value)` | Hash della `key` (`hash(key) % numPartitions`) | **Stessa `key` = stessa partizione** = ordine FIFO garantito e stessa istanza consumer nello stesso gruppo! | `BasicProducer`, `lab2/Producer`, `exam-2024/Producer` |
| `new ProducerRecord<>(topic, value)` | `key = null` $\to$ Sticky/Round-robin partitioner | Massimo bilanciamento, ma **nessuna località per chiave**. | — |
| `new ProducerRecord<>(topic, partition, key, value)` | Partizione forzata esplicitamente (`0, 1, ...`) | Scavalca l'hash della chiave. | — |

#### 2) Pattern di Invio (Asincrono, Callback, Sincrono, Flush, Transazionale)
1. **Asincrono puro (Fire-and-Forget)**:
   ```java
   producer.send(record);
   ```
   *(Rif: `BasicProducer` con `waitAck=false`, `exam-2023/Producer`, `exam-2024/Producer`, `lab2/Producer`)*
2. **Asincrono con Callback (non bloccante con gestione errori)**:
   ```java
   producer.send(record, (metadata, exception) -> {
       if (exception != null) exception.printStackTrace();
       else System.out.println("Partition: " + metadata.partition() + ", Offset: " + metadata.offset());
   });
   ```
   *(Rif: [`BasicProducerTemplate`](../kafka-lab-template/src/main/java/com/polimi/kafka/template/BasicProducerTemplate.java))*
3. **Sincrono per singolo record (`Future.get()`)**:
   ```java
   RecordMetadata ack = producer.send(record).get();
   // ack.topic(), ack.partition(), ack.offset()
   ```
   *(Rif: `BasicProducer` con `waitAck=true`, `BasicProducerTemplate`)*
4. **Invio batch asincrono + attesa collettiva sincrona (`flush()`)**:
   ```java
   for (ConsumerRecord<String, Integer> r : records) {
       producer.send(new ProducerRecord<>(outputTopic, r.key(), sum));
   }
   producer.flush(); // Blocca finché tutti i send() nel buffer non hanno ricevuto ACK
   ```
   *(Rif: `lab2/ConsumersXX.java` `Merger`, `AtLeastOnceForwarderTemplate`)*
5. **Invio atomico transazionale**:
   ```java
   producer.beginTransaction();
   producer.send(record1);
   producer.send(record2);
   producer.commitTransaction(); // oppure producer.flush(); producer.abortTransaction();
   ```
   *(Rif: `TransactionalProducer`, `AtomicUpperCaseConsumer`, `lab2/Validator`)*

---

### 6.2 Tutte le Modalità di LETTURA (`KafkaConsumer`)

#### 1) Iscrizione Dinamica (`subscribe`) vs. Assegnazione Manuale (`assign`)
| Modalità | Codice | Quando si usa | Riferimento Repo |
| :--- | :--- | :--- | :--- |
| **Singolo topic (Consumer Group)** | `consumer.subscribe(Collections.singletonList("inputTopic"));` | Caso standard: Kafka distribuisce le partizioni tra i consumer dello stesso `group.id`. | `BasicConsumer`, `ManualCommitConsumerTemplate` |
| **Multi-topic (Join per chiave)** | `consumer.subscribe(Arrays.asList("sensors1", "sensors2"));` | Un unico consumer legge entrambi i topic. Se hanno lo **stesso numero di partizioni**, le partizioni corrispondenti della stessa chiave vanno alla stessa istanza! | [`Merger` (Lab 2)](../lab/lab2/NSDS26_kafka_lab2/src/main/java/it/polimi/middleware/kafka/eval/ConsumersXX.java) |
| **Con `ConsumerRebalanceListener`** | `consumer.subscribe(topics, new ConsumerRebalanceListener() { onPartitionsRevoked(...); onPartitionsAssigned(...); });` | Forwarder **stateful** multi-istanza: ricostruisce lo stato in RAM ogni volta che cambiano le partizioni assegnate. | [`StatefulForwarderTemplate`](../kafka-lab-template/src/main/java/com/polimi/kafka/template/StatefulForwarderTemplate.java), [`AtLeastOnceForwarderTemplate`](../kafka-lab-template/src/main/java/com/polimi/kafka/template/AtLeastOnceForwarderTemplate.java) |
| **Assegnazione manuale statica** | `List<PartitionInfo> infos = c.partitionsFor(topic);`<br>`c.assign(partitions); c.seekToBeginning(partitions);` | Consumer di **recovery** (senza `group.id`!): legge tutte le partizioni dall'offset `0` senza causare rebalance sul gruppo principale. | [`AtomicFaultTolerantForwarder`](../lab/lab1/src/main/java/it/polimi/middleware/kafka/ex4/AtomicFaultTolerantForwarder.java) |

#### 2) Iterazione sui Record dopo `poll()`
- **Iterazione piatta** (quando ti servono tutti i record in sequenza):
  ```java
  ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMinutes(5));
  for (ConsumerRecord<String, Integer> record : records) {
      String topic = record.topic();
      int partition = record.partition();
      long offset = record.offset();
      String key = record.key();
      Integer value = record.value();
  }
  ```
- **Iterazione partizione per partizione** (necessaria per calcolare `lastOffset + 1`, `firstOffset`, o fare replay limitato):
  ```java
  for (TopicPartition partition : records.partitions()) {
      List<ConsumerRecord<String, String>> pRecords = records.records(partition);
      long firstOffset = pRecords.get(0).offset();
      long lastOffset = pRecords.get(pRecords.size() - 1).offset();
      offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
  }
  ```

#### 3) Controllo Puntuale di Cursore e Offset (`seek`, `committed`, `position`, `pause`)
- **`consumer.seek(partition, offset)`**: riporta indietro il cursore (es. dopo `abortTransaction()`).
- **`consumer.seekToBeginning(partitions)` / `seekToEnd(partitions)`**: salta a offset `0` o alla fine del log.
- **`consumer.committed(partitions)`**: legge gli ultimi offset committati sul broker per quelle partizioni (usato da `Merger` in Lab 2 per capire fin dove fare replay!).
- **`consumer.position(tp)` & `consumer.endOffsets(partitions)` & `consumer.pause(...)`**: controllano dove si trova il cursore locale rispetto alla fine della partizione e mettono in pausa le partizioni già recuperate.
- **`consumer.commitSync()`** *(tutto il batch)* vs **`consumer.commitSync(Map<TopicPartition, OffsetAndMetadata>)`** *(offset espliciti, es. singolo record)* vs **`consumer.commitAsync((offsets, ex) -> ...)`** *(non bloccante, senza retry)*.

---

## 7. Regole d'Oro per le Domande Teoriche nei Commenti dell'Esame

Negli esami (`exam-2023`, `exam-2024`, `lab2`) viene sempre chiesto di compilare in testa al file o nel `README.md`:

1. **Numero di partizioni `(min, max)` per `inputTopic` / `outputTopic`**:
   - Di norma `min = 1`, `max = N` (qualsiasi $N \ge 1$) se il calcolo è stateless o partizionabile per chiave.
   - Se due topic vanno letti insieme dalla stessa istanza per joinare la stessa chiave (es. `sensors1` e `sensors2` in Lab 2): **entrambi devono avere lo stesso numero di partizioni $P$**!
2. **Numero di istanze `(min, max)` di un Consumer/Forwarder**:
   - **Caso A — Stateless o Stateful per chiave (`key`)**: `min = 1`, `max = numPartitions(inputTopic)`. Tutte le istanze devono avere lo **stesso `group.id`**; se usano transazioni, ognuna deve avere un **`transactional.id` univoco**! Oltre $P$ istanze, quelle extra restano *idle*.
   - **Caso B — Stato/Finestra GLOBALE cross-key** (es. `Consumer1` Exam 2024 che somma tutti i messaggi a blocchi di 10 indipendentemente dalla chiave, oppure `PopularTopicConsumer` Exam 2023 che conta i messaggi su tutti i topic): `min = 1`, `max = 1`! Perché un'istanza deve vedere tutto il flusso globale.
3. **Relazione tra `group.id` di componenti DIVERSI**:
   - Due componenti che svolgono ruoli diversi sullo stesso topic (es. `Consumer1` e `Consumer2` in Exam 2024, oppure `PrintConsumer` e `UpperCaseConsumer` in Lab 1 Ex 1) devono avere **`group.id` DIVERSI** per lavorare in **Broadcast** (ciascuno riceve tutti i messaggi).
   - Più repliche dello stesso componente per scalare in parallelo devono avere lo **STESSO `group.id`** per lavorare in **Load Balancing**.
4. **Checklist Rapida Tipi & Serializers (`org.apache.kafka.common.serialization.*`)**:
   - `String` $\leftrightarrow$ `StringSerializer` / `StringDeserializer`
   - `Integer` $\leftrightarrow$ `IntegerSerializer` / `IntegerDeserializer`
   - `Long` $\leftrightarrow$ `LongSerializer` / `LongDeserializer`
   - `Double` $\leftrightarrow$ `DoubleSerializer` / `DoubleDeserializer`
   - `byte[]` $\leftrightarrow$ `ByteArraySerializer` / `ByteArrayDeserializer`
5. **Comandi Rapidi WSL / PowerShell (da `kafka/`)**:
   - Avvio broker: `.\start-kafka.ps1` | Reset completo (cancella topic/offset/tx): `.\reset-kafka.ps1` | Stop: `.\stop-kafka.ps1`
   - Compilazione: `mvn clean compile`
   - Lista topic: `wsl /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`
   - Crea topic: `wsl /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic inputTopic --partitions 2 --replication-factor 1`
   - Leggi topic da console: `wsl /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic outputTopic --from-beginning --property print.key=true`
