# nsds — Akka, Kafka & Node-RED

Repository for **Networked Software for Distributed Systems (NSDS)** at **Politecnico di Milano**, organized into three self-contained sections:

- **[`akka/`](./akka/)**: Akka Classic (`akka-actor_2.13`) templates, lab solutions (`lab1-solutions`, `lab2-solutions`), past exams (`past-exam-2022`, `past-exam-2023`), lecture notes, and a ready-to-use **`akka/akka-exam/`** module.
- **[`kafka/`](./kafka/)**: Apache Kafka (`kafka-clients`) templates ([`kafka-lab-template/`](./kafka/kafka-lab-template/README.md)), Lab 1 exercises (`lab/lab1`), lecture slides/notes, and a ready-to-use **`kafka/kafka-exam/`** module.
- **[`nodered/`](./nodered/)**: Node-RED (`v5.0.9`) + Mosquitto MQTT setup, lecture flow templates ([`nodered-lab-template/`](./nodered/nodered-lab-template/README.md)), lecture notes, PowerShell/IDE launch scripts, and a ready-to-use **`nodered/nodered-exam/`** workspace.

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
├── kafka/
│   ├── pom.xml                   # Kafka parent POM
│   ├── README.md                 # Kafka setup, CLI commands & quick start
│   ├── kafka-lab-template/       # Ready-to-use Kafka templates + README index
│   ├── kafka-exam/               # Ready-to-use module for the Kafka exam
│   ├── lab/                      # Kafka Lab exercises & solutions
│   ├── lectures/                 # Kafka lecture slides
│   └── notes/                    # Kafka lecture notes
└── nodered/
    ├── README.md                 # Node-RED + Mosquitto setup & IDE guide
    ├── package.json              # Node-RED & MQTT dependencies + npm scripts
    ├── start-nodered.ps1         # Start Node-RED (exam / templates) + Mosquitto
    ├── start-mqtt.ps1            # Start Mosquitto MQTT broker in WSL2 (localhost:1883)
    ├── nodered-lab-template/     # Reusable flow templates & UDP/MQTT test scripts
    ├── nodered-exam/             # Ready-to-use workspace for the Node-RED exam
    ├── lab/                      # Node-RED lab materials
    ├── lectures/                 # Node-RED lecture slides (1.1–1.3, 2.1–2.2)
    └── notes/                    # Node-RED lecture notes & exam playbook
```

---

## 🚀 Quick Build & Run

Compile all Akka and Kafka modules from the root directory:

```powershell
mvn clean test-compile
```

Start Node-RED + Mosquitto MQTT broker:

```powershell
.\nodered\start-nodered.ps1 -Mode exam
# or with all lecture templates loaded:
.\nodered\start-nodered.ps1 -Mode templates
```

