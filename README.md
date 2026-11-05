# NSDS Akka Classic

This repository contains two Maven projects for **Networked Software for Distributed Systems (NSDS)** at **Politecnico di Milano**:
1. **`akka-lab-template/`**: Base project template for Akka Classic (`akka-actor_2.13`) applications.
2. **`lab1-solutions/`**: Solutions for Lab 1 exercises.

---

## 📁 Repository Structure

```text
nsds-akka-local/
├── README.md
├── notes/
│   └── Akka_Lecture_Notes.md
├── akka-lab-template/
│   ├── pom.xml
│   └── src/main/
│       ├── resources/
│       │   └── remote.conf
│       └── java/com/polimi/akka/template/
│           ├── MainTemplate.java
│           ├── TemplateActor.java
│           ├── TemplateMessage.java
│           ├── StateMachineActorTemplate.java
│           ├── StashActorTemplate.java
│           ├── RequestReplyTemplate.java
│           ├── SupervisionTemplate.java
│           └── RemoteDistributionTemplate.java
└── lab1-solutions/
    ├── pom.xml
    └── src/main/java/com/polimi/nsds/akka/
        ├── base/
        │   ├── SimpleMessage.java
        │   ├── CounterActor.java
        │   └── Counter.java
        ├── ex1/
        │   ├── classes/
        │   │   ├── IncrementMsg.java
        │   │   ├── DecrementMsg.java
        │   │   ├── CounterActor.java
        │   │   └── Counter.java
        │   └── selectors/
        │       ├── CounterMsg.java
        │       ├── CounterActor.java
        │       └── Counter.java
        ├── ex2/
        │   ├── IncrementMsg.java
        │   ├── DecrementMsg.java
        │   ├── CounterActor.java
        │   └── CounterEx2.java
        └── ex3/
            ├── PutMsg.java
            ├── GetMsg.java
            ├── ReplyMsg.java
            ├── ServerActor.java
            ├── ClientActor.java
            ├── ClientAskActor.java
            └── ContactListApp.java
```

---

## 🛠️ Prerequisites

- **Java JDK**: 17 or higher
- **Apache Maven**: 3.8+
- **IDE**: IntelliJ IDEA or Visual Studio Code (with *Extension Pack for Java*)

---

## 🚀 Build & Run

### 1. Exam Templates (`akka-lab-template`)

```bash
cd akka-lab-template
mvn clean compile
```

- **PowerShell:**
  ```powershell
  mvn exec:java '-Dexec.mainClass=com.polimi.akka.template.MainTemplate'
  mvn exec:java '-Dexec.mainClass=com.polimi.akka.template.StateMachineActorTemplate'
  mvn exec:java '-Dexec.mainClass=com.polimi.akka.template.StashActorTemplate'
  mvn exec:java '-Dexec.mainClass=com.polimi.akka.template.RequestReplyTemplate'
  mvn exec:java '-Dexec.mainClass=com.polimi.akka.template.SupervisionTemplate'
  ```
- **Bash / CMD:**
  ```bash
  mvn exec:java -Dexec.mainClass="com.polimi.akka.template.MainTemplate"
  mvn exec:java -Dexec.mainClass="com.polimi.akka.template.StateMachineActorTemplate"
  mvn exec:java -Dexec.mainClass="com.polimi.akka.template.StashActorTemplate"
  mvn exec:java -Dexec.mainClass="com.polimi.akka.template.RequestReplyTemplate"
  mvn exec:java -Dexec.mainClass="com.polimi.akka.template.SupervisionTemplate"
  ```

---

### 2. Lab 1 Solutions (`lab1-solutions`)

```bash
cd lab1-solutions
mvn clean compile
```

#### Base Example (`base`)
- **PowerShell:**
  ```powershell
  mvn exec:java '-Dexec.mainClass=com.polimi.nsds.akka.base.Counter'
  ```
- **Bash / CMD:**
  ```bash
  mvn exec:java -Dexec.mainClass="com.polimi.nsds.akka.base.Counter"
  ```

#### Exercise 1A (`ex1.classes`)
- **PowerShell:**
  ```powershell
  mvn exec:java '-Dexec.mainClass=com.polimi.nsds.akka.ex1.classes.Counter'
  ```
- **Bash / CMD:**
  ```bash
  mvn exec:java -Dexec.mainClass="com.polimi.nsds.akka.ex1.classes.Counter"
  ```

#### Exercise 1B (`ex1.selectors`)
- **PowerShell:**
  ```powershell
  mvn exec:java '-Dexec.mainClass=com.polimi.nsds.akka.ex1.selectors.Counter'
  ```
- **Bash / CMD:**
  ```bash
  mvn exec:java -Dexec.mainClass="com.polimi.nsds.akka.ex1.selectors.Counter"
  ```

#### Exercise 2 (`ex2`)
- **PowerShell:**
  ```powershell
  mvn exec:java '-Dexec.mainClass=com.polimi.nsds.akka.ex2.CounterEx2'
  ```
- **Bash / CMD:**
  ```bash
  mvn exec:java -Dexec.mainClass="com.polimi.nsds.akka.ex2.CounterEx2"
  ```

#### Exercise 3 (`ex3`)
- **PowerShell:**
  ```powershell
  mvn exec:java '-Dexec.mainClass=com.polimi.nsds.akka.ex3.ContactListApp'
  # With "ask" argument:
  mvn exec:java '-Dexec.mainClass=com.polimi.nsds.akka.ex3.ContactListApp' '-Dexec.args=ask'
  ```
- **Bash / CMD:**
  ```bash
  mvn exec:java -Dexec.mainClass="com.polimi.nsds.akka.ex3.ContactListApp"
  # With "ask" argument:
  mvn exec:java -Dexec.mainClass="com.polimi.nsds.akka.ex3.ContactListApp" -Dexec.args="ask"
  ```

---

## 💻 Running from an IDE

- **IntelliJ IDEA**: Right-click on `akka-lab-template/pom.xml` or `lab1-solutions/pom.xml`, select **"Add as Maven Project"**, open any class with a `main` method, and click **Run (▶)**.
- **VS Code**: Open the repository folder, ensure the **Extension Pack for Java** is installed, open any class with a `main` method, and click **Run** above `main`.

