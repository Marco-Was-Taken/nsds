# Kafka Lab & Exam Templates — Indice Rapido

Questa cartella (`com.polimi.kafka.template`) raccoglie **8 template pronti all'uso** aggiornati dopo il Lab 1 di Apache Kafka (NSDS — Politecnico di Milano), pensati per essere copiati e adattati rapidamente in `kafka-exam`.

---

## 🧭 Quale Template Usare? (Tabella Decisionale)

| Scenario / Requisito | Template da Usare | Garanzia / Meccanismo | Configurazioni Critiche |
| :--- | :--- | :--- | :--- |
| **Creare/resettare topic con $P$ partizioni o Log Compaction** | [`TopicAdminTemplate.java`](./src/main/java/com/polimi/kafka/template/TopicAdminTemplate.java) | Gestione programmatica topic | `AdminClient`, `NewTopic`, `CLEANUP_POLICY_COMPACT` |
| **Producer semplice o Idempotente** | [`BasicProducerTemplate.java`](./src/main/java/com/polimi/kafka/template/BasicProducerTemplate.java) | Async / Sync ACK (`waitAck`) / No duplicati per retry | `ENABLE_IDEMPOTENCE_CONFIG`, `future.get()`, `key` per località |
| **Producer con scritture atomiche (commit/abort)** | [`TransactionalProducerTemplate.java`](./src/main/java/com/polimi/kafka/template/TransactionalProducerTemplate.java) | Tutto-o-niente su più record/topic | `TRANSACTIONAL_ID_CONFIG`, `initTransactions()`, `flush()` prima di `abortTransaction()` |
| **Consumer base (Auto-Commit)** | [`BasicConsumerTemplate.java`](./src/main/java/com/polimi/kafka/template/BasicConsumerTemplate.java) | Nessuna garanzia forte (possibili duplicati/perdite) | `GROUP_ID_CONFIG` (uguale = load balance, diverso = broadcast), `AUTO_OFFSET_RESET_CONFIG` |
| **Consumer At-Least-Once (Commit Manuale)** | [`ManualCommitConsumerTemplate.java`](./src/main/java/com/polimi/kafka/template/ManualCommitConsumerTemplate.java) | At-Least-Once | `ENABLE_AUTO_COMMIT_CONFIG = false` + `consumer.commitSync()` |
| **Consumer a valle di Producer/Forwarder Transazionali** | [`TransactionalConsumerTemplate.java`](./src/main/java/com/polimi/kafka/template/TransactionalConsumerTemplate.java) | Legge solo record committati | `ISOLATION_LEVEL_CONFIG = "read_committed"` |
| **Forwarder Stateless Read–Process–Write (EOS)** | [`TransactionalForwarderTemplate.java`](./src/main/java/com/polimi/kafka/template/TransactionalForwarderTemplate.java) | End-to-End Exactly-Once (Stateless) | `read_committed`, `enable.auto.commit=false`, `sendOffsetsToTransaction(lastOffset + 1)`, `seek` su errore |
| **Forwarder Stateful Fault-Tolerant (EOS + Stato in RAM)** | [`StatefulForwarderTemplate.java`](./src/main/java/com/polimi/kafka/template/StatefulForwarderTemplate.java) | End-to-End Exactly-Once (Stateful, single & multi-instance) | `outputTopic` compattato + `storeConsumer` (`assign` + `seekToBeginning`) + `ConsumerRebalanceListener` |

---

## ▶️ Comandi Rapidi di Esecuzione (PowerShell da `kafka/`)

Assicurati prima che il broker sia avviato (`..\kafka\start-kafka.ps1` o `.\start-kafka.ps1` dalla cartella `kafka/`) e compila con `mvn clean compile`.

```powershell
# 1. Crea/ricrea topic (es. inputTopic con 2 partizioni, outputTopic compattato con 2 partizioni)
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.TopicAdminTemplate' '-Dexec.args=inputTopic 2 1 false'
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.TopicAdminTemplate' '-Dexec.args=outputTopic 2 1 true'

# 2. Basic / Idempotent Producer (args: topic idempotent)
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.BasicProducerTemplate' '-Dexec.args=inputTopic true'

# 3. Transactional Producer (args: topic transactionalId)
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.TransactionalProducerTemplate' '-Dexec.args=inputTopic txProducer-1'

# 4. Basic Consumer (args: groupId topic offsetReset)
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.BasicConsumerTemplate' '-Dexec.args=groupA inputTopic latest'

# 5. Manual Commit Consumer (args: groupId topic offsetReset)
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.ManualCommitConsumerTemplate' '-Dexec.args=manualGroup inputTopic earliest'

# 6. Transactional Consumer (args: groupId topic readUncommitted)
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.TransactionalConsumerTemplate' '-Dexec.args=txGroup outputTopic false'

# 7. Stateless EOS Forwarder (args: transactionalId groupId inputTopic outputTopic)
#    Per più istanze in parallelo: STESSO groupId, transactionalId DIVERSO!
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.TransactionalForwarderTemplate' '-Dexec.args=forwarderTx-1 forwarderGroup inputTopic outputTopic'

# 8. Stateful Fault-Tolerant EOS Forwarder (args: transactionalId groupId inputTopic outputTopic)
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.StatefulForwarderTemplate' '-Dexec.args=statefulTx-1 statefulGroup inputTopic outputTopic'
```

---

## ⚠️ Regole d'Oro & Trappole Comuni (Quick Cheatsheet)

1. **`lastOffset + 1` in `sendOffsetsToTransaction`**: Kafka salva l'offset del **prossimo** record da leggere (`lastOffset + 1`), non dell'ultimo letto.
2. **Zombie Fencing (`transactional.id`)**: se avvii $N$ istanze dello stesso forwarder nello stesso `group.id`, **ogni istanza deve avere un `transactional.id` univoco** (`args[0]`), altrimenti l'ultima avviata butta fuori la precedente (`ProducerFencedException`).
3. **`ISOLATION_LEVEL_CONFIG` di default è `"read_uncommitted"`**: su ogni consumer che legge da un producer/forwarder transazionale imposta sempre `"read_committed"`.
4. **`auto.offset.reset` (`"earliest"` vs `"latest"`)**: vale **solo** alla prima esecuzione di un `group.id` nuovo. Se il gruppo ha già committato offset sul broker, li ignora e riparte dall'ultimo offset salvato (cambia `group.id` o usa `reset-kafka.ps1`).
5. **Parallelismo Consumer**: il numero massimo di consumer attivi in uno stesso `group.id` è pari al numero di partizioni $P$ del topic. Consumer extra ($C > P$) restano *idle*.
6. **Stateful Forwarder multi-istanza**: senza `ConsumerRebalanceListener.onPartitionsAssigned(...)` che ricarica lo stato da `outputTopic`, in caso di crash/rebalance l'istanza superstite erediterebbe partizioni senza avere in RAM lo stato recente prodotto dall'istanza caduta.
7. **Serializer/Deserializer coerenti**: `StringSerializer`/`StringDeserializer`, `IntegerSerializer`/`IntegerDeserializer`, `LongSerializer`/`LongDeserializer`, `DoubleSerializer`/`DoubleDeserializer`.
