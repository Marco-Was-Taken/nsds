# nsds — Akka & Kafka

Repository for **Networked Software for Distributed Systems (NSDS)** at **Politecnico di Milano**, organized into two self-contained sections:

- **[`akka/`](./akka/)**: Akka Classic (`akka-actor_2.13`) templates, lab solutions (`lab1-solutions`, `lab2-solutions`), past exams (`past-exam-2022`, `past-exam-2023`), lecture notes, and a ready-to-use **`akka/akka-exam/`** module.
- **[`kafka/`](./kafka/)**: Apache Kafka (`kafka-clients`) templates (`kafka-lab-template`), folders for lectures/labs/notes, and a ready-to-use **`kafka/kafka-exam/`** module.

---

## 📁 Repository Structure

```text
nsds/
├── pom.xml                       # Root Maven aggregator (akka + kafka)
├── README.md
├── akka/
│   ├── pom.xml                   # Akka parent POM
│   ├── README.md
│   ├── akka-lab-template/        # Reusable Akka actor & pattern templates
│   ├── akka-exam/                # Ready-to-use module for the Akka exam
│   ├── lab1-solutions/           # Lab 1 solutions (Ex 1, 2, 3)
│   ├── lab2-solutions/           # Lab 2 solutions (Ex 4, 5, 6)
│   ├── past-exam-2022/           # 2022 Evaluation Lab + solution
│   ├── past-exam-2023/           # 2023 Evaluation Lab + solution
│   ├── lab/                      # Akka lab PDFs
│   ├── lectures/                 # Akka lecture slides
│   └── notes/                    # Akka lecture notes
└── kafka/
    ├── pom.xml                   # Kafka parent POM
    ├── README.md
    ├── kafka-lab-template/       # Reusable Producer, Consumer & EOS Forwarder templates
    ├── kafka-exam/               # Ready-to-use module for the Kafka exam
    ├── lab/                      # Kafka lab PDFs
    ├── lectures/                 # Kafka lecture slides
    └── notes/                    # Kafka lecture notes
```

---

## 🚀 Quick Build

Compile all Akka and Kafka modules from the root directory:

```powershell
mvn clean test-compile
```
