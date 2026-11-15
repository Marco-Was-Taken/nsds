# Kafka Lab & Exam Templates — Indice Rapido

Questa cartella (`com.polimi.kafka.template`) raccoglie **13 template pronti all'uso** per coprire tutte le garanzie di consegna (**None**, **At-Most-Once**, **At-Least-Once**, **Exactly-Once**) su **Producer**, **Consumer** e **Forwarder**, pensati per essere copiati e adattati rapidamente in `kafka-exam`.

> **Appunti completi da esame**: consulta [`Kafka_Delivery_Guarantees_Cheatsheet.md`](../notes/Kafka_Delivery_Guarantees_Cheatsheet.md) (matrice garanzie, configurazioni `ProducerConfig`/`ConsumerConfig`, modalità di lettura/invio, trappole d'esame) e [`Kafka_Lecture_Notes.md`](../notes/Kafka_Lecture_Notes.md).

---

## 🧭 Quale Template Usare? (Tabella Decisionale)

| Ruolo & Garanzia | Template da Usare | Meccanismo / Configurazioni Critiche |
| :--- | :--- | :--- |
| **Admin: Creare/resettare topic o Log Compaction** | [`TopicAdminTemplate.java`](./src/main/java/com/polimi/kafka/template/TopicAdminTemplate.java) | `AdminClient`, `NewTopic`, `CLEANUP_POLICY_COMPACT` |
| **Producer: None / At-Most-Once / At-Least-Once / Idempotent EOS** | [`BasicProducerTemplate.java`](./src/main/java/com/polimi/kafka/template/BasicProducerTemplate.java) | `ENABLE_IDEMPOTENCE_CONFIG` (`false` esplicito in 3.5.1 per None/AMO/ALO!), `ACKS_CONFIG`, `RETRIES_CONFIG`, `async` / `callback` / `sync` (`.get()`) / `flush` |
| **Producer: Exactly-Once Transazionale (Multi-Record)** | [`TransactionalProducerTemplate.java`](./src/main/java/com/polimi/kafka/template/TransactionalProducerTemplate.java) | `TRANSACTIONAL_ID_CONFIG`, `initTransactions()`, `begin/commitTransaction()`, `flush()` prima di `abortTransaction()` |
| **Consumer: None (Auto-Commit)** | [`BasicConsumerTemplate.java`](./src/main/java/com/polimi/kafka/template/BasicConsumerTemplate.java) | `ENABLE_AUTO_COMMIT_CONFIG = true` (possibili duplicati o perdite) |
| **Consumer: At-Most-Once (0 o 1 volta)** | [`AtMostOnceConsumerTemplate.java`](./src/main/java/com/polimi/kafka/template/AtMostOnceConsumerTemplate.java) | `ENABLE_AUTO_COMMIT_CONFIG = false` + `commitSync(...)` **PRIMA** di elaborare/stampare (per-record o per-batch) |
| **Consumer: At-Least-Once (1+ volte)** | [`ManualCommitConsumerTemplate.java`](./src/main/java/com/polimi/kafka/template/ManualCommitConsumerTemplate.java) | `ENABLE_AUTO_COMMIT_CONFIG = false` + `commitSync(...)` **DOPO** aver elaborato, `ISOLATION_LEVEL_CONFIG = "read_committed"` |
| **Consumer: Transazionale (`read_committed` vs `read_uncommitted`)** | [`TransactionalConsumerTemplate.java`](./src/main/java/com/polimi/kafka/template/TransactionalConsumerTemplate.java) | `ISOLATION_LEVEL_CONFIG = "read_committed"` (scarta record di transazioni abortite/aperte) |
| **Forwarder: None (Best-Effort)** | [`BasicForwarderTemplate.java`](./src/main/java/com/polimi/kafka/template/BasicForwarderTemplate.java) | Consumer `auto.commit = true` + Producer `acks = "0"`, `idempotence = false` |
| **Forwarder: At-Most-Once** | [`AtMostOnceForwarderTemplate.java`](./src/main/java/com/polimi/kafka/template/AtMostOnceForwarderTemplate.java) | Consumer `commitSync(...)` **PRIMA** del forward + Producer `retries = "0"`, `idempotence = false`, `acks = "1"` |
| **Forwarder: At-Least-Once (Stateless o Stateful con Replay)** | [`AtLeastOnceForwarderTemplate.java`](./src/main/java/com/polimi/kafka/template/AtLeastOnceForwarderTemplate.java) | **`producer.flush()` PRIMA di `consumer.commitSync()`** + `ConsumerRebalanceListener` con replay `[0, committed)` senza duplicare output |
| **Forwarder: Exactly-Once Stateless (EOS)** | [`TransactionalForwarderTemplate.java`](./src/main/java/com/polimi/kafka/template/TransactionalForwarderTemplate.java) | `read_committed`, `enable.auto.commit=false`, `sendOffsetsToTransaction(lastOffset + 1)`, `seek` su `KafkaException` |
| **Forwarder: Exactly-Once Tumbling Window (EOS)** | [`WindowedForwarderTemplate.java`](./src/main/java/com/polimi/kafka/template/WindowedForwarderTemplate.java) | `window` fuori da `while(true)`, transazione solo a finestra piena, commit dei soli offset in finestra |
| **Forwarder: Exactly-Once Stateful Fault-Tolerant (EOS)** | [`StatefulForwarderTemplate.java`](./src/main/java/com/polimi/kafka/template/StatefulForwarderTemplate.java) | `outputTopic` compattato + `storeConsumer` (`assign` + `seekToBeginning`) + `ConsumerRebalanceListener` + `pendingUpdates` |

---

## ▶️ Comandi Rapidi di Esecuzione (PowerShell da `kafka/`)

Assicurati prima che il broker sia avviato (`.\start-kafka.ps1`) e compila con `mvn clean compile`.

```powershell
# 1. Crea/ricrea topic
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.TopicAdminTemplate' '-Dexec.args=inputTopic 2 1 false'
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.TopicAdminTemplate' '-Dexec.args=outputTopic 2 1 true'

# 2. Producer per garanzia (args: topic guarantee[none|at-most-once|at-least-once|exactly-once] sendMode[sync|async|callback|flush])
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.BasicProducerTemplate' '-Dexec.args=inputTopic exactly-once sync'

# 3. Transactional Producer (args: topic transactionalId)
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.TransactionalProducerTemplate' '-Dexec.args=inputTopic txProducer-1'

# 4. Consumer: None / At-Most-Once / At-Least-Once / Transactional
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.BasicConsumerTemplate' '-Dexec.args=groupA inputTopic latest'
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.AtMostOnceConsumerTemplate' '-Dexec.args=amoGroup inputTopic earliest'
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.ManualCommitConsumerTemplate' '-Dexec.args=manualGroup inputTopic earliest'
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.TransactionalConsumerTemplate' '-Dexec.args=txGroup outputTopic false'

# 5. Forwarder: None / At-Most-Once / At-Least-Once / EOS Stateless / EOS Windowed / EOS Stateful
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.BasicForwarderTemplate' '-Dexec.args=noneFwdGroup inputTopic outputTopic'
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.AtMostOnceForwarderTemplate' '-Dexec.args=amoFwdGroup inputTopic outputTopic'
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.AtLeastOnceForwarderTemplate' '-Dexec.args=aloFwdGroup inputTopic outputTopic'
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.TransactionalForwarderTemplate' '-Dexec.args=forwarderTx-1 forwarderGroup inputTopic outputTopic'
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.WindowedForwarderTemplate' '-Dexec.args=windowedTx-1 windowedGroup inputTopic outputTopic'
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.StatefulForwarderTemplate' '-Dexec.args=statefulTx-1 statefulGroup inputTopic outputTopic'
```

---

## ⚠️ Regole d'Oro & Trappole Comuni (Quick Cheatsheet)

1. **Default Kafka 3.5.1 sul Producer**: `enable.idempotence` è `"true"` di default! Per configurazioni **None**, **At-Most-Once** o **At-Least-Once** puro devi impostare `ENABLE_IDEMPOTENCE_CONFIG = "false"`.
2. **`lastOffset + 1` in `sendOffsetsToTransaction` e `commitSync(Map)`**: Kafka salva l'offset del **prossimo** record da leggere (`lastOffset + 1`), non dell'ultimo letto.
3. **`commitSync()` senza argomenti**: committa **tutto** l'ultimo `poll()`, non solo i record elaborati finora nel ciclo `for`! Per committare per singolo record o finestra usa sempre `commitSync(Map<TopicPartition, OffsetAndMetadata>)`.
4. **Zombie Fencing (`transactional.id`)**: se avvii $N$ istanze dello stesso forwarder nello stesso `group.id`, **ogni istanza deve avere un `transactional.id` univoco** (`args[0]`), altrimenti l'ultima avviata butta fuori la precedente (`ProducerFencedException`).
5. **`ISOLATION_LEVEL_CONFIG` di default è `"read_uncommitted"`**: su ogni consumer che legge da un producer/forwarder transazionale imposta sempre `"read_committed"`.
6. **`auto.offset.reset` (`"earliest"` vs `"latest"`)**: vale **solo** alla prima esecuzione di un `group.id` nuovo. Se il gruppo ha già committato offset sul broker, li ignora e riparte dall'ultimo offset salvato (cambia `group.id` o usa `reset-kafka.ps1`).
7. **Parallelismo Consumer**: il numero massimo di consumer attivi in uno stesso `group.id` è pari al numero di partizioni $P$ del topic. Consumer extra ($C > P$) restano *idle*.
8. **`poll(Duration)`**: ritorna **immediatamente** appena almeno 1 record è disponibile, non attende mai il timeout per riempire finestre! Dichiara sempre finestre/stati **fuori** dal `while (true)`.

