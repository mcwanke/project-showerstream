# PROJECT_SPEC.md — Shower Detection Pipeline

## Current Phase: Phase 1

---

## Problem Statement

A single whole-house water meter measures total household water flow. There are 3 bathrooms, each with a shower, but no per-bathroom water sensors. The goal is to infer which shower is running by correlating the water meter signal with per-bathroom environmental sensors (humidity, temperature, lights), attribute each shower session to a specific bathroom, and calculate per-shower water usage and estimated cost over daily and weekly windows.

---

## Sensor Inventory

### Water Meter (whole-house)
- `sensor.water_flow_rate` — current flow rate in gal/min (real-time)
- `sensor.water_volume` — cumulative volume in gallons; resets to 0 when flow has been 0 for 15 consecutive seconds

The 15-second zero-flow reset is handled in Home Assistant and is the canonical session boundary signal. This means the water volume sensor effectively gives per-session volume automatically.

### Bathrooms

All 3 bathrooms have:
- `sensor.bathX_humidity` — current relative humidity (%)
- `sensor.bathX_temperature` — current temperature

Most bathrooms also have:
- Smart light state (on/off) for 1-2 lights per bathroom
- Fan on/off state (not used for detection, used for post-shower automation)

**Important:** One bathroom has no smart lights. Light state is therefore a positive signal but never a required one.

### Home Assistant Helpers (pre-existing)
- `sensor.bathX_humidity_average` — 12-hour rolling average humidity per bathroom

These are already exposed as HA sensors and will be streamed alongside raw readings. They serve as the dynamic baseline threshold for humidity confirmation — this approach automatically adapts to seasonal and environmental humidity changes without requiring manual recalibration.

---

## Session Boundary Logic

A shower session is defined as:

- **Opens:** Water flow rate rises above the minimum flow threshold (configurable, `FLOW_ZERO_TIMEOUT_SECONDS`)
- **Closes:** Water flow rate drops to 0 and remains at 0 for 15 consecutive seconds

This logic is already implemented in Home Assistant via the water volume sensor reset. The pipeline respects this boundary — when `sensor.water_volume` resets to 0, the current session is closed and a session event is emitted to `home.showers`.

### Edge Cases

| Case | Handling |
|---|---|
| Toilet flush or tap before shower | Flow hits zero between events; 15s timer separates correctly |
| Back-to-back showers | Zero-flow timeout separates them into distinct sessions |
| Overlapping showers (two simultaneous) | Not handled. First attribution wins for the duration. Acceptable limitation. |
| Pre-shower tap blip merged into shower | Not a concern — volume contribution is negligible |

---

## Attribution Logic

Attribution determines which bathroom a shower session belongs to. It uses a **confidence scoring model** evaluated continuously during the session. The bathroom with the highest score above the attribution threshold wins.

### Hard Gate

Water flow rate must be above the minimum threshold to open a session. If flow is 0, no scoring runs.

### Confidence Score Components

| Signal | Weight | Notes |
|---|---|---|
| Water meter slope increasing | High | Flow rate actively rising confirms water is running |
| Humidity slope rising steeply (bathroom X) | High | Primary attribution signal. Unique fingerprint for shower activity. |
| Humidity above 12h rolling average (bathroom X) | Medium | Confirms elevated state, but slope is more important than absolute level |
| Lights on (bathroom X) | Medium | Strong positive signal but not required. One bathroom has no smart lights. |
| No competing humidity slopes in other bathrooms | Low | Increases confidence when other bathrooms are clearly inactive |

### Humidity Slope Calculation

Slope is calculated as the rate of humidity change over a **3-minute rolling window**. A steep positive slope (shower) is distinguishable from:
- Flat/declining elevated humidity (residual from prior shower or environmental cause)
- Gradual ambient humidity rise (not a shower)

Threshold for "steep" slope is configurable and should be tuned against real session data after Phase 1.

### Timing

Based on observed data, humidity in the active bathroom crosses the 12h rolling average threshold within approximately **2 minutes** of shower start. The confirmation window is set to `HUMIDITY_CONFIRMATION_WINDOW_SECONDS` (default: 180 seconds).

### Attribution States

Each session moves through these states:

```
PENDING → ATTRIBUTED (temporary) → CONFIRMED
                                  ↓
                             CONFIRMED_FINAL (on session close)
```

- **PENDING:** Flow started, no bathroom has cleared the score threshold yet. Falls back to priority order if window expires.
- **ATTRIBUTED (temporary):** One bathroom has the highest score but humidity hasn't confirmed yet.
- **CONFIRMED:** Humidity slope has confirmed the attribution. High confidence.
- **CONFIRMED_FINAL:** Session closed (flow → 0). Final event emitted to `home.showers`.

### Bathroom Priority Order (tiebreaker / fallback)

If no bathroom clears the confidence threshold within the confirmation window, the session is attributed to the highest-priority bathroom with lights on, or the default priority order if no lights signal is available. Priority order is configurable in `.env`.

---

## Kafka Topic Design

### `home.water`

Water meter readings. Produced on every state change from HA.

```json
{
  "timestamp": "2024-01-15T20:41:00.000Z",
  "flow_rate_gpm": 2.3,
  "volume_gallons": 12.4,
  "session_active": true
}
```

### `home.bathrooms`

All bathroom sensor events. Partitioned by `bathroom_id` to preserve ordering per bathroom.

```json
{
  "timestamp": "2024-01-15T20:43:00.000Z",
  "bathroom_id": "bath1",
  "humidity": 78.5,
  "humidity_avg_12h": 64.2,
  "temperature": 72.1,
  "lights_on": true
}
```

### `home.showers`

Detected shower sessions. Written by the stream processor when a session closes.

```json
{
  "session_id": "uuid",
  "bathroom_id": "bath1",
  "started_at": "2024-01-15T20:41:00.000Z",
  "ended_at": "2024-01-15T21:00:00.000Z",
  "duration_seconds": 1140,
  "volume_gallons": 57.2,
  "attribution_state": "CONFIRMED",
  "confidence_score": 0.87,
  "cost_estimate": null
}
```

Note: `cost_estimate` is null here and populated by the cost-aggregator consumer.

---

## Service Design

### ha-producer

**Purpose:** Subscribe to the Home Assistant WebSocket API and produce sensor events to the appropriate Kafka topics.

**Design principles:**
- Thin — no business logic
- Subscribes to HA state_changed events and filters for relevant entities
- Routes water meter events to `home.water`, bathroom sensor events to `home.bathrooms`
- Reconnects automatically on WebSocket disconnect

**Key dependencies:** `websockets`, `kafka-python`, `python-dotenv`

---

### shower-detector (Faust)

**Purpose:** Consume `home.water` and `home.bathrooms`, run the session state machine, emit detected sessions to `home.showers`.

**Design principles:**
- Stateful — maintains per-session state in Faust tables
- Evaluates confidence scores continuously while a session is open
- Emits a session event to `home.showers` on session close
- One Faust agent per topic consumed

**Key dependencies:** `faust-streaming`, `python-dotenv`

---

### influxdb-sink

**Purpose:** Consume `home.showers` and write session records to InfluxDB for historical analysis.

**InfluxDB details:**
- Version: 2.7
- Client library: `influxdb-client`
- Bucket: configurable via `INFLUXDB_BUCKET` env var
- Measurement: `shower_sessions`
- Tags: `bathroom_id`, `attribution_state`
- Fields: `duration_seconds`, `volume_gallons`, `confidence_score`, `cost_estimate`

---

### cost-aggregator

**Purpose:** Consume `home.showers` and produce daily/weekly rollups per bathroom vs. household total.

**Outputs:**
- Per-bathroom daily volume and estimated cost
- Household daily total
- Per-bathroom weekly volume and estimated cost
- Household weekly total
- Per-bathroom % of household usage

**Cost calculation:** `volume_gallons × WATER_COST_PER_GALLON`

Rollups are written to InfluxDB under measurement `shower_cost_rollups`.

---

## Docker Compose Architecture

All services run on the pre-existing external Docker network `network_apps`.

Services:
- `redpanda` — Kafka broker + Redpanda Console UI (port 8080)
- `ha-producer` — built from `./ha-producer`
- `shower-detector` — built from `./shower-detector`
- `influxdb-sink` — built from `./consumers/influxdb-sink`
- `cost-aggregator` — built from `./consumers/cost-aggregator`

InfluxDB and Grafana are **not** defined in this compose file — they run in a separate existing stack and are reachable via `network_apps` by service name.

---

## Environment Variable Reference

```
# Home Assistant
HA_URL=ws://homeassistant.local:8123/api/websocket
HA_TOKEN=your_long_lived_token_here

# Kafka / Redpanda
KAFKA_BOOTSTRAP=redpanda:9092

# InfluxDB
INFLUXDB_URL=http://influxdb:8086
INFLUXDB_TOKEN=your_influxdb_token_here
INFLUXDB_ORG=your_org
INFLUXDB_BUCKET=home_water

# Water cost
WATER_COST_PER_GALLON=0.01

# Session logic
FLOW_ZERO_TIMEOUT_SECONDS=15
HUMIDITY_CONFIRMATION_WINDOW_SECONDS=180
MIN_FLOW_RATE_GPM=0.5

# Attribution fallback priority (comma-separated bathroom IDs, first = highest priority)
BATHROOM_PRIORITY_ORDER=bath1,bath2,bath3
```

---

## Home Assistant Entity Map

Update this table to match actual HA entity IDs before running Phase 1.

| Bathroom ID | Humidity | Temperature | Humidity Avg 12h | Lights |
|---|---|---|---|---|
| `bath1` | `sensor.bath1_humidity` | `sensor.bath1_temperature` | `sensor.bath1_humidity_average` | TBD |
| `bath2` | `sensor.bath2_humidity` | `sensor.bath2_temperature` | `sensor.bath2_humidity_average` | TBD |
| `bath3` | `sensor.bath3_humidity` | `sensor.bath3_temperature` | `sensor.bath3_humidity_average` | None |

Water meter:
- Flow rate: `sensor.water_flow_rate` *(update to actual entity ID)*
- Volume: `sensor.water_volume` *(update to actual entity ID)*

---

## Design Decisions Log

| Decision | Rationale |
|---|---|
| Redpanda over vanilla Kafka | Single binary, Kafka-compatible, built-in UI — much friendlier for homelab |
| Faust over Kafka Streams | Python-native, lower barrier to entry, sufficient for this scale |
| Confidence scoring over binary logic | One bathroom has no smart lights; scoring handles missing signals gracefully |
| Humidity slope over threshold crossing | Slope is a more reliable real-time signal; threshold crossing has too much lag and ambient interference |
| 3-minute confirmation window | Based on observed data: humidity crosses threshold within ~2 minutes of shower start |
| Output topic (`home.showers`) | Decouples detection from downstream consumers; consumers don't need to know detection logic |
| InfluxDB over TimescaleDB | Already running in homelab; purpose-built for time-series; avoids adding another database |
| Overlapping showers ignored | Rare edge case; first attribution wins; not worth the complexity to solve now |
| 15s zero-flow timeout delegated to HA | Already implemented and working; no reason to reimplement in the pipeline |