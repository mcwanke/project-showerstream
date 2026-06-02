# ShowerStream

A Kafka-based home sensor pipeline that figures out which shower is running using
environmental data — because I had one water meter, three bathrooms, and a weekend.

Built to learn Kafka/Redpanda in a real project context, and to solve an actual data
problem: attributing household water usage to specific bathrooms without per-room
water sensors.

---

## The Problem

My house has a single whole-house water meter and three bathrooms. The meter tells me
water is flowing, but not where. Each bathroom has humidity, temperature, and (in most
cases) smart light sensors — but no water sensor of its own.

The goal: correlate the water meter signal with per-bathroom environmental data to
infer which shower is running, attribute each session to a bathroom, and calculate
per-shower water usage and estimated cost.

---

## How It Works

```
Home Assistant ──► ha-producer ──► Redpanda (Kafka)
                                        │
                              ┌─────────┴──────────┐
                         home.water          home.bathrooms
                              └─────────┬──────────┘
                                        ▼
                                shower-detector
                                        │
                           ┌────────────┴────────────┐
                      home.showers         home.shower_scores
                           └────────────┬────────────┘
                                        ▼
                                 influxdb-sink
                                        │
                                   InfluxDB 2.7
                                        │
                                     Grafana
```

**ha-producer** subscribes to Home Assistant's WebSocket API and routes sensor events
to the appropriate Kafka topics — no business logic, just routing.

**shower-detector** runs a session state machine. When water flow opens a session, it
continuously scores each bathroom using seven weighted signals:

| Signal | Weight |
|---|---|
| Humidity rise rate (peak, span-3 lookback) | 30% |
| Humidity delta above 12h rolling average | 25% |
| Temperature rise rate (peak, span-3 lookback) | 15% |
| Temperature delta above 12h rolling average | 15% |
| Shower light on during session | 5% |
| Room light on during session | 5% |
| Fan on during session | 5% |

At session close, the bathroom with the highest score above the confidence threshold
wins. Attribution is labeled CONFIRMED (≥ 0.6), ATTRIBUTED (≥ 0.3), or FALLBACK.
Session volume is computed via trapezoidal integration of the flow rate signal.

**influxdb-sink** consumes session events and score timelines and writes them to
InfluxDB for historical analysis and Grafana dashboards.

---

## Tech Stack

| Component | Technology |
|---|---|
| Message broker | Redpanda (Kafka-compatible) |
| Stream processing | Python / aiokafka |
| Sensor source | Home Assistant WebSocket API |
| Storage | InfluxDB 2.7 |
| Visualization | Grafana |
| Containerization | Docker / Docker Compose |

---

## Kafka Topics

| Topic | Description |
|---|---|
| `home.water` | Water meter readings (flow rate, volume) |
| `home.bathrooms` | Per-bathroom sensor events (humidity, temp, lights, fan) |
| `home.showers` | Attributed shower sessions on session close |
| `home.shower_scores` | Per-bathroom confidence scores emitted every 30s during active sessions |

---

## Setup

### Prerequisites

- Docker + Docker Compose
- Home Assistant instance with long-lived access token
- InfluxDB 2.7 (existing instance, reachable via Docker network)
- Grafana (existing instance)
- External Docker network named `network_apps`

### Configuration

**1. Copy and fill in environment variables:**
```bash
cp .env.example .env
```

Key variables to set:

| Variable | Description |
|---|---|
| `HA_URL` | Home Assistant WebSocket URL |
| `HA_TOKEN` | HA long-lived access token |
| `KAFKA_BOOTSTRAP` | Redpanda broker address |
| `INFLUXDB_URL` | InfluxDB 2.x URL |
| `INFLUXDB_TOKEN` | InfluxDB API token |
| `INFLUXDB_ORG` | InfluxDB organization |
| `INFLUXDB_BUCKET` | Target bucket |
| `WATER_COST_PER_GALLON` | Local water rate for cost calculations |

**2. Create your sensor entity map:**
```bash
cp config.example.yaml config.yaml
# edit config.yaml with your actual Home Assistant entity IDs
```

`config.yaml` is gitignored — it contains your actual HA entity IDs and is never committed.

### Run

```bash
docker compose up -d --build
```

Redpanda Console is available at `http://localhost:8180` once running.

---

## Repo Structure

```
project-showerstream/
├── ha-producer/          # HA WebSocket → Kafka producer
├── shower-detector/      # Session state machine + attribution scorer
├── influxdb-sink/        # Kafka → InfluxDB consumer
├── config.example.yaml   # Sanitized entity map template
├── .env.example          # Environment variable template
└── docker-compose.yml    # Reference stack definition
```

---

## Project Spec

Full architecture detail, design decisions, and scoring model rationale are in
[PROJECT_SPEC.md](PROJECT_SPEC.md).
