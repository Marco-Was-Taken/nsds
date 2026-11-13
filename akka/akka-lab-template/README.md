# Akka Classic Lab & Exam Templates — Indice Rapido

Questa cartella (`com.polimi.akka.template`) raccoglie **10 file template** pronti all'uso per i laboratori (Lab 1 & Lab 2) e gli esami di **Akka Classic** (NSDS — Politecnico di Milano), pensati per essere copiati e adattati rapidamente in `akka/akka-exam`.

---

## 🧭 Quale Template Usare? (Tabella Decisionale)

| Scenario / Requisito | Template da Usare | Meccanismo Akka | Rif. Lab / Appunti |
| :--- | :--- | :--- | :--- |
| **Messaggi immutabili e serializzabili** | [`TemplateMessage.java`](./src/main/java/com/polimi/akka/template/TemplateMessage.java) | `private final`, defensive copy, `Serializable` | Lab 1 — Pattern 1 |
| **Attore base + selector/predicati + lifecycle hooks** | [`TemplateActor.java`](./src/main/java/com/polimi/akka/template/TemplateActor.java) | `.match(Class, cond, fn)`, `Props.create`, `sender().tell(...)` | Lab 1 Ex 1 — Pattern 1 |
| **Bootstrap `main()` + thread pool + `Patterns.ask`** | [`MainTemplate.java`](./src/main/java/com/polimi/akka/template/MainTemplate.java) | `ActorSystem`, `ExecutorService`, `Await.result` | Lab 1 — Sezione 2 |
| **Macchina a stati finiti (cambio comportamento)** | [`StateMachineActorTemplate.java`](./src/main/java/com/polimi/akka/template/StateMachineActorTemplate.java) | `getContext().become(nextState())` | Pattern 2 |
| **Bufferizzare messaggi quando una precondizione fallisce** | [`StashActorTemplate.java`](./src/main/java/com/polimi/akka/template/StashActorTemplate.java) | `AbstractActorWithStash`, `stash()`, `unstashAll()` | Lab 1 Ex 2 — Pattern 3 |
| **Client-Server Request/Reply (Async vs Sync)** | [`RequestReplyTemplate.java`](./src/main/java/com/polimi/akka/template/RequestReplyTemplate.java) | `become(waiting) + stash()` vs `Patterns.ask` | Lab 1 Ex 3 — Pattern 4A/4B |
| **Ritardo non-bloccante + Sleep/Wakeup buffering** | [`SchedulerAndBusyStashTemplate.java`](./src/main/java/com/polimi/akka/template/SchedulerAndBusyStashTemplate.java) | `scheduler().scheduleOnce(...)`, `become(busy/sleeping)`, `stash()` | Lab 2 Ex 4 & Ex 6 — Pattern 3B/4C |
| **Supervisione (`resume` / `restart` / `stop` / `escalate`)** | [`SupervisionTemplate.java`](./src/main/java/com/polimi/akka/template/SupervisionTemplate.java) | `OneForOneStrategy`, `AllForOneStrategy`, `DeciderBuilder` | Lab 2 Ex 5 — Pattern 5 |
| **Dispatcher/Router Supervisore (Round-Robin & Load Balancing)** | [`SupervisorDispatcherTemplate.java`](./src/main/java/com/polimi/akka/template/SupervisorDispatcherTemplate.java) | `preStart()` + `actorOf`, `OneForOneStrategy.resume()`, `become()` | Evaluation Lab — Pattern 7 |
| **Distribuzione remota su TCP** | [`RemoteDistributionTemplate.java`](./src/main/java/com/polimi/akka/template/RemoteDistributionTemplate.java) | `remote.conf`, `ConfigFactory`, `actorSelection("akka.tcp://...")` | Pattern 6 |

---

## ▶️ Comandi Rapidi di Esecuzione (PowerShell da `akka/akka-lab-template`)

```powershell
mvn clean compile

mvn exec:java '-Dexec.mainClass=com.polimi.akka.template.MainTemplate'
mvn exec:java '-Dexec.mainClass=com.polimi.akka.template.StateMachineActorTemplate'
mvn exec:java '-Dexec.mainClass=com.polimi.akka.template.StashActorTemplate'
mvn exec:java '-Dexec.mainClass=com.polimi.akka.template.RequestReplyTemplate'
mvn exec:java '-Dexec.mainClass=com.polimi.akka.template.SchedulerAndBusyStashTemplate'
mvn exec:java '-Dexec.mainClass=com.polimi.akka.template.SupervisionTemplate'
mvn exec:java '-Dexec.mainClass=com.polimi.akka.template.SupervisorDispatcherTemplate'
```
