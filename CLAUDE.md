# CLAUDE.md

## How to Work With Me

This file is consumed by AI-assisted code generation tooling at the start of every session. Read this file completely before writing or modifying any code. For full architecture detail, see PROJECT_SPEC.md. 

Never open responses with filler phrases like "Great question!", "Of course!", "Certainly!", or similar warmups. Start every response with the actual answer. No preamble, no acknowledgment of the question. 

Match response length to task complexity. Simple questions get direct, short answers. Complex tasks get full, detailed responses. Never pad responses with restatements of the question or closing sentences that repeat what you just said. 

Before any significant task, show me 2-3 ways you could approach this work. Wait for me to choose before proceeding. 

If you are uncertain about any fact, statistic, date, or piece of technical information: say so explicitly before including it. Never fill gaps in your knowledge with plausible-sounding information. When in doubt, say so.

I am learning and using this as a way to grow my knowledge. I have a background in software engineernig and a strong grasp of the fundamentals. Assume I am still learning the technology in the project and I will state when my comfort level with the topic is strong enough to skip over elements. Adjust the depth of every response to match this. Never over-explain what I already know. Never skip context I need.

NEVER create, write, or modify any file without explicit user approval. State what you intend to write and wait for confirmation before touching disk.

## Behavior

Only modify files, functions, and lines of code directly related to the current task. Do not refactor, rename, reorganize, reformat, or "improve" anything I did not explicitly ask you to change. If you notice something worth fixing elsewhere, mention it in a note at the end. Do not touch it. Ever.

Before making any change that significantly alters content I've already created (rewriting sections, removing paragraphs, restructuring flow, changing tone): stop. Describe exactly what you're about to change and why. Wait for my confirmation before proceeding.

Before deleting any file, overwriting existing code, dropping database records, or removing dependencies: stop. List exactly what will be affected. Ask for explicit confirmation. Only proceed after I say yes in the current message. "You mentioned this earlier" is not confirmation.

The following require explicit in-session confirmation, no exceptions: deploying or pushing to any environment, running migrations or schema changes, sending any external API call, executing any command with irreversible side effects. I must say yes in the current message.

The following also require explicit in-session confirmation before executing: any shell command run via terminal (including read-only commands like ls, git status, docker ps). State what you intend to run and why. Wait for my approval.

After any coding task, end with: Files changed (list every file touched) / What was modified (one line per file) / Files intentionally not touched / Follow-up needed.

Never send, post, publish, share, or schedule anything on my behalf without my explicit confirmation in the current message. This includes emails, calendar invites, document shares, or any action outside this conversation. I must say yes in the current message.

For any task involving architecture decisions, debugging complex issues, or non-trivial features: work through the problem step by step before writing any code. Show your reasoning. Identify where you're uncertain. Then implement.

## Memory

Project decisions and deferred work are tracked in `memory/`. This folder is gitignored — it does not get committed. Check `memory/MEMORY.md` for an index before starting significant work.

Read memory/MEMORY.md at the start of every session. Never contradict a logged decision without flagging it first.

When I say "END SESSION" or "end session": ask me "Ready to write session summary to memory/MEMORY.md?", provide a short bullet point summary list of what you would write, and wait for confirmation before writing. Include: Worked on / Completed / In progress / Decisions made / Next session priorities. Once this is done remind me to commit to github if needed.

Maintain a file called memory/ERRORS.md. When an approach takes more than 2 attempts to work, ask me "Ready to log this to memory/ERRORS.md?" and wait for confirmation before writing. Check memory/ERRORS.md before suggesting approaches to similar tasks.

For questions involving system architecture, performance tradeoffs, database design, or long-term technical decisions: reason through the problem step by step before answering. Surface tradeoffs I haven't considered. Flag assumptions that might not hold at scale. Then give your recommendation.

## Core Rules

1. Ask, don't assume. If something is unclear, ask before writing a single line. Never make silent assumptions about intent, architecture, or requirements.

2. Simplest solution first. Always implement the simplest thing that could work. Do not add abstractions or flexibility that weren't explicitly requested.

3. Don't touch unrelated code. If a file or function is not directly part of the current task, do not modify it, even if you think it could be improved.

4. Flag uncertainty explicitly. If you are not confident about an approach or technical detail, say so before proceeding. Confidence without certainty causes more damage than admitting a gap.


## Project Overview

This is a Kafka-based home sensor data pipeline that detects which shower is running in a 3-bathroom house using Home Assistant sensor data. It correlates a single whole-house water meter with per-bathroom humidity, temperature, and light sensors to attribute shower sessions to specific bathrooms, then calculates per-shower water usage and estimated cost over daily/weekly windows.

The full design rationale and architecture decisions are documented in `PROJECT_SPEC.md`. Read it before making significant changes.

---

## Repo Structure

```
shower-pipeline/
├── CLAUDE.md                  # This file
├── PROJECT_SPEC.md            # Full design spec and architecture decisions
├── docker-compose.yml         # Full stack definition
├── .env.example               # Environment variable template (never commit .env)
├── ha-producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py            # HA WebSocket → Kafka producer
├── shower-detector/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── detector.py            # Faust stream processor / session state machine
└── consumers/
    ├── influxdb-sink/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── sink.py            # Writes all session events to InfluxDB
    └── cost-aggregator/
        ├── Dockerfile
        ├── requirements.txt
        └── aggregator.py      # Daily/weekly usage and cost rollups
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Message broker | Redpanda (Kafka-compatible) |
| Stream processing | Faust (Python) |
| Producer | kafka-python + websockets |
| Storage | InfluxDB 2.7 (existing instance) |
| Visualization | Grafana (existing instance) |
| Containerization | Docker / Docker Compose |
| Language | Python 3.11+ |
| Docker network | `network_apps` (external, pre-existing) |

---

## Environment Variables

All configuration lives in `.env`. Never hardcode values. See `.env.example` for all required variables.

Key variables:
- `HA_URL` — Home Assistant WebSocket URL (e.g. `ws://homeassistant.local:8123/api/websocket`)
- `HA_TOKEN` — Home Assistant long-lived access token
- `KAFKA_BOOTSTRAP` — Redpanda broker address (e.g. `redpanda:9092`)
- `INFLUXDB_URL` — InfluxDB 2.x URL
- `INFLUXDB_TOKEN` — InfluxDB API token
- `INFLUXDB_ORG` — InfluxDB organization name
- `INFLUXDB_BUCKET` — Target bucket (e.g. `home_water`)
- `WATER_COST_PER_GALLON` — Configurable water rate for cost calculations
- `FLOW_ZERO_TIMEOUT_SECONDS` — Seconds of zero flow before session closes (default: 15)
- `HUMIDITY_CONFIRMATION_WINDOW_SECONDS` — Max window to wait for humidity confirmation (default: 180)

---

## Kafka Topics

| Topic | Description | Partitioning |
|---|---|---|
| `home.water` | Water meter readings: flow_rate (gal/min), cumulative_volume (gal) | Single partition |
| `home.bathrooms` | Bathroom sensor events: humidity, temperature, light state | Partitioned by bathroom_id |
| `home.showers` | Detected and attributed shower sessions (output topic) | Single partition |

---

## Bathroom IDs

Bathrooms are identified by string IDs that map to Home Assistant entity names:

| Bathroom ID | HA Entities |
|---|---|
| `bath1` | `sensor.bath1_humidity`, `sensor.bath1_temperature` |
| `bath2` | `sensor.bath2_humidity`, `sensor.bath2_temperature` |
| `bath3` | `sensor.bath3_humidity`, `sensor.bath3_temperature` |

Light entities vary per bathroom — see `PROJECT_SPEC.md` for details. One bathroom has no smart lights.

---

## Session Detection Logic

See `PROJECT_SPEC.md` for the full scoring model. Key rules:

1. Water flow rate crossing the minimum threshold is a **hard gate** — no session opens without it
2. Sessions close after `FLOW_ZERO_TIMEOUT_SECONDS` of sustained zero flow
3. Attribution uses a per-bathroom confidence score — highest scoring bathroom wins
4. Humidity slope (rate of change over a rolling 3-minute window) is the primary attribution signal
5. Lights are a medium-weight signal — one bathroom has no smart lights, so lights are never required
6. Overlapping showers (two running simultaneously) are not handled — first attribution wins

---

## Development Guidelines

- Each service is an independent Python process in its own container
- Services communicate exclusively through Kafka topics — no direct service-to-service calls
- All services must handle Kafka connection failures gracefully with retry logic
- Log to stdout; Docker handles log collection
- Keep producers thin — no business logic in the producer, just routing
- All business logic lives in `shower-detector/detector.py`
- Use type hints throughout
- Each service has its own `requirements.txt` — do not share a monolithic requirements file

---

## Running the Stack

```bash
# Copy and fill in environment variables
cp .env.example .env

# Build and start everything
docker compose up -d --build

# Watch logs for a specific service
docker compose logs -f shower-detector

# Redpanda UI (once running)
open http://localhost:8080
```

---

## Phased Build Plan

This project is being built in phases. Do not skip ahead.

- **Phase 1** — Redpanda + `ha-producer` only. Goal: get real HA events flowing into Kafka topics and visible in the Redpanda UI.
- **Phase 2** — Add `shower-detector`. Goal: session state machine running, attributed sessions appearing in `home.showers`.
- **Phase 3** — Add `influxdb-sink`, `cost-aggregator`, Grafana dashboards, and HA feedback.

The current phase is indicated in `PROJECT_SPEC.md`.