# NSDS Akka Classic - Lab 1 Solutions & Template

This repository contains the complete local implementation of **Akka Classic Lab 1** for the **Networked Software for Distributed Systems (NSDS)** course at **Politecnico di Milano (PoliMi)**.

The project is structured into two completely independent Maven projects under `nsds-akka-local/`:
1. **`akka-lab-template/`**: A clean, reusable base template for Akka Classic (`akka-actor_2.13`) applications.
2. **`lab1-solutions/`**: Fully implemented, documented, and tested solutions for all Lab 1 exercises.

All source code, line-by-line comments, Javadoc explanations, console outputs, and documentation are strictly written in English.

---

## 📁 Repository Structure

```text
nsds-akka-local/
├── README.md                                # Comprehensive guide & execution instructions
├── akka-lab-template/                       # Reusable Akka Classic project template
│   ├── pom.xml                              # Java 17, Akka 2.7.0, Logback 1.2.3, JUnit 4.13.1
│   └── src/main/java/com/polimi/akka/template/
│       ├── TemplateMessage.java             # Immutable serializable message
│       ├── TemplateActor.java               # AbstractActor implementation with props()
│       └── MainTemplate.java                # System bootstrapper, tell(), and termination
└── lab1-solutions/                          # Lab 1 implementations
    ├── pom.xml                              # Same dependency tree for seamless classpath execution
    └── src/main/java/com/polimi/nsds/akka/
        ├── base/                            # Introduction: Concurrent Counter
        │   ├── SimpleMessage.java           # Basic immutable message
        │   ├── CounterActor.java            # Actor with state (explains why locks are NOT needed)
        │   └── Counter.java                 # Multi-threaded stress test (16 threads, 1000 messages)
        ├── ex1/                             # Exercise 1: Increment & Decrement
        │   ├── classes/                     # Solution 1A: Separate message classes
        │   │   ├── IncrementMsg.java        # Immutable class for increment
        │   │   ├── DecrementMsg.java        # Immutable class for decrement
        │   │   ├── CounterActor.java        # Pattern matching with two .match() clauses
        │   │   └── Counter.java             # Parallel exec.submit() testing
        │   └── selectors/                   # Solution 1B: Predicate-based message selectors
        │       ├── CounterMsg.java          # Unified message class with Op enum
        │       ├── CounterActor.java        # Pattern matching using predicates (.match(Class, Predicate, ...))
        │       └── Counter.java             # Parallel exec.submit() testing
        ├── ex2/                             # Exercise 2: Non-negative Counter with Stash
        │   ├── IncrementMsg.java            # Triggers increment and unstashAll()
        │   ├── DecrementMsg.java            # Decrements if counter > 0, otherwise stash()
        │   ├── CounterActor.java            # Extends AbstractActorWithStash
        │   └── CounterEx2.java              # Exact test sequence verifying 1, 0, 1, 0, 1, 0 evolution
        └── ex3/                             # Exercise 3: Client/Server Contact Directory
            ├── PutMsg.java                  # Stores name and email
            ├── GetMsg.java                  # Queries contact by name
            ├── ReplyMsg.java                # Response with email or not-found error
            ├── ServerActor.java             # In-memory HashMap contact store (replies via sender().tell(reply, self()))
            ├── ClientActor.java             # Non-blocking client using getContext().become() + stash()/unstashAll()
            ├── ClientAskActor.java          # Synchronous client using Patterns.ask() + Await.result()
            └── ContactListApp.java          # Single ActorSystem orchestrating the scenario
```

---

## 🛠️ Prerequisites & Environment

- **Java JDK**: Version 17 or higher (tested with JDK 17 and JDK 25).
- **Apache Maven**: Version 3.8+ (tested with Maven 3.9.12).
- **Operating System**: Windows / Linux / macOS.
- **IDE**: VS Code (with *Extension Pack for Java*) or IntelliJ IDEA.

---

## 🚀 How to Compile & Run

### 1. Template Project (`akka-lab-template`)

Navigate to the template directory:
```bash
cd nsds-akka-local/akka-lab-template
```

**Compile:**
```bash
mvn clean compile
```

**Run `MainTemplate`:**
- **PowerShell (Windows):**
  ```powershell
  mvn exec:java '-Dexec.mainClass=com.polimi.akka.template.MainTemplate'
  ```
- **Bash / Command Prompt:**
  ```bash
  mvn exec:java -Dexec.mainClass="com.polimi.akka.template.MainTemplate"
  ```

---

### 2. Lab 1 Solutions (`lab1-solutions`)

Navigate to the solutions directory:
```bash
cd nsds-akka-local/lab1-solutions
```

**Compile all solutions:**
```bash
mvn clean compile
```

#### A. Base Example: Concurrent Counter (`base`)
Tests 16 worker threads submitting 1000 messages concurrently to a single `CounterActor`.
- **PowerShell:**
  ```powershell
  mvn exec:java '-Dexec.mainClass=com.polimi.nsds.akka.base.Counter'
  ```
- **Bash / CMD:**
  ```bash
  mvn exec:java -Dexec.mainClass="com.polimi.nsds.akka.base.Counter"
  ```

#### B. Exercise 1A: Class-Based Matching (`ex1.classes`)
Tests concurrent increment and decrement using distinct message classes (`IncrementMsg` and `DecrementMsg`).
- **PowerShell:**
  ```powershell
  mvn exec:java '-Dexec.mainClass=com.polimi.nsds.akka.ex1.classes.Counter'
  ```
- **Bash / CMD:**
  ```bash
  mvn exec:java -Dexec.mainClass="com.polimi.nsds.akka.ex1.classes.Counter"
  ```

#### C. Exercise 1B: Predicate-Based Selectors (`ex1.selectors`)
Tests concurrent increment and decrement using a unified `CounterMsg` and predicate filtering.
- **PowerShell:**
  ```powershell
  mvn exec:java '-Dexec.mainClass=com.polimi.nsds.akka.ex1.selectors.Counter'
  ```
- **Bash / CMD:**
  ```bash
  mvn exec:java -Dexec.mainClass="com.polimi.nsds.akka.ex1.selectors.Counter"
  ```

#### D. Exercise 2: Non-negative Counter with Stash (`ex2`)
Executes the exact test sequence (`decrement, decrement, increment, decrement, increment, increment`) and verifies the state evolution: `1 -> 0 -> 1 -> 0 -> 1 -> 0`.
- **PowerShell:**
  ```powershell
  mvn exec:java '-Dexec.mainClass=com.polimi.nsds.akka.ex2.CounterEx2'
  ```
- **Bash / CMD:**
  ```bash
  mvn exec:java -Dexec.mainClass="com.polimi.nsds.akka.ex2.CounterEx2"
  ```

#### E. Exercise 3: Client/Server Contact Directory (`ex3`)
Runs `ServerActor` and a client actor (`ClientActor` by default using `become` + `stash`, or `ClientAskActor` when passing `"ask"`), performs insertions (`PutMsg`), queries (`GetMsg`), and displays `ReplyMsg` responses on the console.
- **PowerShell (become + stash workaround):**
  ```powershell
  mvn exec:java '-Dexec.mainClass=com.polimi.nsds.akka.ex3.ContactListApp'
  ```
- **PowerShell (Ask pattern):**
  ```powershell
  mvn exec:java '-Dexec.mainClass=com.polimi.nsds.akka.ex3.ContactListApp' '-Dexec.args=ask'
  ```
- **Bash / CMD (become + stash workaround):**
  ```bash
  mvn exec:java -Dexec.mainClass="com.polimi.nsds.akka.ex3.ContactListApp"
  ```
- **Bash / CMD (Ask pattern):**
  ```bash
  mvn exec:java -Dexec.mainClass="com.polimi.nsds.akka.ex3.ContactListApp" -Dexec.args="ask"
  ```

---

## 💻 Running from VS Code / IntelliJ IDEA

1. **IntelliJ IDEA**:
   - Right-click on `nsds-akka-local/lab1-solutions/pom.xml` (or `akka-lab-template/pom.xml`) and select **"Add as Maven Project"**.
   - Navigate to any main class (e.g. `CounterEx2.java` or `ContactListApp.java`).
   - Click the green **Run (▶)** button located next to `public static void main(String[] args)`.

2. **Visual Studio Code**:
   - Open the folder `nsds-akka-local` in VS Code.
   - Install the **Extension Pack for Java** (if not already installed).
   - Open any file containing a `main` method (e.g., `ContactListApp.java`).
   - Click the **"Run"** CodeLens action appearing directly above the `main` method.

---

## 📚 Deep-Dive Concept Explanations

### 1. Why Locks / Synchronization are NOT Needed in `CounterActor`
In standard Java shared-memory programming, incrementing an integer (`++counter`) is a non-atomic read-modify-write operation requiring `synchronized` blocks, locks, or `AtomicInteger`.
In the **Akka Actor Model** (Lecture 1, Slides 8 & 13):
- Actors may modify their own private state, but can only affect each other through messages, avoiding the need for any locks.
- Messages are processed **atomically**: *"as if the entire processing of a message was executed in a synchronized method of a class where all methods are synchronized"*.
- Messages received meanwhile are queued up in the actor's mailbox and do not affect the currently running receive functionality.

### 2. Class-Based Matching vs Predicate-Based Selectors (Exercise 1)

Per Lecture 1, Slide 22 (*"Mailbox semantics"*), when an actor receives a message:
- It checks all the `match` clauses in the order in which they are defined;
- It uses the method associated to the first matching clause;
- If no matching clause exists, the message is discarded.

| Feature | Class-Based Matching (Ex 1A) | Predicate-Based Selectors (Ex 1B) |
| :--- | :--- | :--- |
| **Syntax** | `.match(IncrementMsg.class, this::onIncrement)` | `.match(CounterMsg.class, msg -> msg.getOp() == ..., this::onIncrement)` |
| **Clause Evaluation** | Clauses checked in definition order by message class. | Clauses checked in definition order by message class + boolean predicate. |
| **Type Safety** | Strong compile-time validation. An invalid operation cannot be constructed. | Checked at runtime. An unhandled op value falls through and is discarded. |
| **Schema Separation** | Each message class carries only the fields relevant to its operation. | Single message class must carry the fields for all variants. |
| **Best Use Cases** | Fixed set of protocol commands or request/reply pairs (`PutMsg`, `GetMsg`, `ReplyMsg`). | Value-based routing (e.g., op codes, thresholds) or variants sharing identical fields. |

### 3. Step-by-Step Stash Trace & State Evolution (Exercise 2)

The laboratory specification requires that the counter state is never negative, and that the test sequence (`decrement, decrement, increment, decrement, increment, increment`) evolves as **1, 0, 1, 0, 1, 0** using `AbstractActorWithStash` (`stash()` and `unstashAll()`, Lecture 2, Slide 9):

1. **Initial State:** `counter = 0`, `Stash = []`.
2. **Send Decrement 1:** `counter == 0`, actor calls `stash()`. Stash: `[Dec1]`, counter: `0`.
3. **Send Decrement 2:** `counter == 0`, actor calls `stash()`. Stash: `[Dec1, Dec2]`, counter: `0`.
4. **Send Increment 1:** `counter` increments from $0 \to 1$ (**State: 1**). The actor calls `unstashAll()`, which extracts `[Dec1, Dec2]` in the same order in which they were added and prepends them to the mailbox.
5. **Process Dec1 (from Mailbox):** `counter` is $1 > 0$, so decrement succeeds (`1 -> 0`, **State: 0**).
6. **Process Dec2 (from Mailbox):** `counter` is now $0$, so the actor calls `stash()` again. Stash: `[Dec2]`, counter: `0`.
7. **Send Decrement 3:** `counter == 0`, actor calls `stash()`. Stash: `[Dec2, Dec3]`, counter: `0`.
8. **Send Increment 2:** `counter` increments from $0 \to 1$ (**State: 1**). The actor calls `unstashAll()`, prepending `[Dec2, Dec3]` to the mailbox.
9. **Process Dec2 (from Mailbox):** `counter` is $1 > 0$, so decrement succeeds (`1 -> 0`, **State: 0**).
10. **Process Dec3 (from Mailbox):** `counter == 0`, actor calls `stash()`. Stash: `[Dec3]`, counter: `0`.
11. **Send Increment 3:** `counter` increments from $0 \to 1$ (**State: 1**). The actor calls `unstashAll()`, prepending `[Dec3]` to the mailbox.
12. **Process Dec3 (from Mailbox):** `counter` is $1 > 0$, so decrement succeeds (`1 -> 0`, **State: 0**).

**Output Sequence Observed:**
```text
Counter is 0: decrement stashed
Counter is 0: decrement stashed
Counter increased to: 1
Counter decreased to: 0
Counter is 0: decrement stashed
Counter is 0: decrement stashed
Counter increased to: 1
Counter decreased to: 0
Counter is 0: decrement stashed
Counter increased to: 1
Counter decreased to: 0
```
This confirms the expected `1, 0, 1, 0, 1, 0` state evolution.

### 4. Client/Server Contact Directory Architecture (Exercise 3)

- **`PutMsg(name, email)`**: Asynchronous command to store a contact in the server.
- **`GetMsg(name)`**: Asynchronous query asking for the email corresponding to `name`.
- **`ReplyMsg(name, email, found, statusMessage)`**: Response sent by `ServerActor` to the requester using `sender().tell(reply, self())` (Lecture 1, Slide 26 & Lecture 2, Slide 11).
- **Encapsulation (`ServerActor`):** Manages its contact table using a regular `java.util.HashMap<String, String>` without locks because message processing is atomic.
- **Two Client Implementations (Lecture 2, Slides 10 & 11):**
  1. **`ClientActor` (Workaround with `become` + `stash`, Slide 10):** After sending a `GetMsg` via `server.tell(msg, self())`, the client calls `getContext().become(waitingForReply())` to match only `ReplyMsg` and `stash()` all other messages. Upon receiving `ReplyMsg`, it calls `unstashAll()` and `getContext().become(idle())`.
  2. **`ClientAskActor` (Ask Pattern, Slide 11):** Sends `GetMsg` with `Patterns.ask(server, msg, TIMEOUT_MS)` to obtain a `Future<Object>` and blocks with `Await.result(future, timeout)` to achieve synchronous request-reply behavior.

---
*Created for Politecnico di Milano - Networked Software for Distributed Systems.*
