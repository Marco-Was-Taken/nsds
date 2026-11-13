# NSDS Apache Kafka

Questa cartella contiene i moduli Maven, gli script di avvio e il materiale per la parte di **Apache Kafka** del corso **Networked Software for Distributed Systems (NSDS)** al **Politecnico di Milano**.

---

## 🛠️ Ambiente pre-installato (WSL2 + Windows)

Sul tuo PC è già stato installato e configurato tutto il necessario dentro **WSL2 (Ubuntu)**:
- **Java Runtime**: OpenJDK 17 (`/usr/bin/java`)
- **Apache Kafka 3.5.1** (Scala 2.13): `/opt/kafka`
- **Configurazione Broker**: `/opt/kafka/config/server.properties` (già configurato per esporre `localhost:9092` verso Windows/IntelliJ IDEA).

I tuoi programmi Java (`Producer`, `Consumer`, ecc.) girano direttamente su **Windows** (da **IntelliJ IDEA** o da terminale PowerShell) collegandosi a `localhost:9092`, mentre il broker Kafka gira in WSL2 senza i bug di file-locking tipici di Windows.

---

## 🚀 Avvio, Arresto e Reset Rapido (da PowerShell)

Dalla cartella `kafka/` (o dalla root del repository) puoi gestire ZooKeeper + Kafka Broker con un solo comando:

### 1. Avviare ZooKeeper e Kafka
```powershell
.\kafka\start-kafka.ps1
```

### 2. Fermare Kafka e ZooKeeper
```powershell
.\kafka\stop-kafka.ps1
```

### 3. Reset completo (cancella tutti i topic, messaggi, offset e transazioni)
Utilissimo tra un esercizio e l'altro o durante l'esame per ripartire da uno stato pulito:
```powershell
.\kafka\reset-kafka.ps1
```

---

## 💻 Avvio Manuale su Terminale WSL (Alternativa didattica)

Se preferisci vedere i log in tempo reale in due terminali separati:

1. **Terminale 1 — Avvia ZooKeeper**:
   ```powershell
   wsl /opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
   ```
2. **Terminale 2 — Avvia Kafka Broker**:
   ```powershell
   wsl /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
   ```

---

## 🔍 Comandi CLI Utili (Topic, Messaggi, Consumer Group)

Puoi eseguire i tool CLI di Kafka direttamente da PowerShell anteponendo `wsl /opt/kafka/bin/...`:

- **Elencare i topic esistenti**:
  ```powershell
  wsl /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
  ```
- **Creare un topic con $N$ partizioni** (es. `inputTopic` con 2 partizioni):
  ```powershell
  wsl /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic inputTopic --partitions 2 --replication-factor 1
  ```
- **Descrivere un topic (partizioni, leader, ISR)**:
  ```powershell
  wsl /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic inputTopic
  ```
- **Leggere i messaggi di un topic da console**:
  ```powershell
  wsl /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic outputTopic --from-beginning --property print.key=true
  ```
- **Ispezionare gli offset di un Consumer Group**:
  ```powershell
  wsl /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group groupA
  ```

---

## 📚 Template Pronti per Lab & Esame (`kafka-lab-template`)

Tutti i template riutilizzabili (aggiornati dopo il Lab 1) sono raccolti e indicizzati in **[`kafka-lab-template/README.md`](./kafka-lab-template/README.md)**:

- [`TopicAdminTemplate`](./kafka-lab-template/src/main/java/com/polimi/kafka/template/TopicAdminTemplate.java) — Creazione/reset topic, partizioni e *Log Compaction*
- [`BasicProducerTemplate`](./kafka-lab-template/src/main/java/com/polimi/kafka/template/BasicProducerTemplate.java) — Producer base (async/sync ACK) e *Idempotent Producer*
- [`TransactionalProducerTemplate`](./kafka-lab-template/src/main/java/com/polimi/kafka/template/TransactionalProducerTemplate.java) — Producer transazionale (`commitTransaction` / `flush` + `abortTransaction`)
- [`BasicConsumerTemplate`](./kafka-lab-template/src/main/java/com/polimi/kafka/template/BasicConsumerTemplate.java) — Consumer base con *Auto-Commit*
- [`ManualCommitConsumerTemplate`](./kafka-lab-template/src/main/java/com/polimi/kafka/template/ManualCommitConsumerTemplate.java) — Consumer *At-Least-Once* con `commitSync()`
- [`TransactionalConsumerTemplate`](./kafka-lab-template/src/main/java/com/polimi/kafka/template/TransactionalConsumerTemplate.java) — Consumer transazionale (`read_committed` vs `read_uncommitted`)
- [`TransactionalForwarderTemplate`](./kafka-lab-template/src/main/java/com/polimi/kafka/template/TransactionalForwarderTemplate.java) — Forwarder *Stateless* End-to-End Exactly-Once (`sendOffsetsToTransaction` + `seek`)
- [`StatefulForwarderTemplate`](./kafka-lab-template/src/main/java/com/polimi/kafka/template/StatefulForwarderTemplate.java) — Forwarder *Stateful* Fault-Tolerant EOS (recovery da topic compattato + `ConsumerRebalanceListener`)

---

## ▶️ Eseguire i Template e l'Esame

### Da IntelliJ IDEA
1. Avvia il broker con `.\kafka\start-kafka.ps1`.
2. Apri qualsiasi classe con metodo `main` (in `kafka-lab-template` o in `kafka/kafka-exam`) e clicca su **Run (▶)**.
   - Se vuoi lanciare più istanze dello stesso Consumer nello stesso Consumer Group, in IntelliJ vai su **Edit Configurations → Modify options → Allow multiple instances**.

### Da Terminale (Maven)
```powershell
cd kafka
mvn clean compile

# 1. Crea/resetta i topic di test
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.TopicAdminTemplate' '-Dexec.args=inputTopic 2 1 false'

# 2. Avvia il Consumer di esempio
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.BasicConsumerTemplate'

# 3. Avvia il Producer di esempio (in un altro terminale)
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.BasicProducerTemplate'

# 4. Avvia il Forwarder Transazionale Stateless (Exactly-Once)
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.TransactionalForwarderTemplate'

# 5. Avvia il Forwarder Stateful Fault-Tolerant (Exactly-Once con stato su topic compattato)
mvn exec:java -pl kafka-lab-template '-Dexec.mainClass=com.polimi.kafka.template.StatefulForwarderTemplate'
```

