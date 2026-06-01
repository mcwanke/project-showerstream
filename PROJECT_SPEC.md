# PROJECT_SPEC.md — Shower Detection Pipeline

## Current Phase: Phase 3 (in progress)

- Phase 1 (ha-producer) — complete
- Phase 2 (shower-detector) — complete
- Phase 3 (influxdb-sink, Grafana dashboards) — next

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

- **Opens:** `flow_rate_gpm` rises to or above `MIN_FLOW_RATE_OPEN` (default: 0.5 GPM)
- **Closes:** `flow_rate_gpm` drops below `MIN_FLOW_RATE_CLOSE` (default: 0.1 GPM) and remains there for `FLOW_ZERO_TIMEOUT_SECONDS` (default: 10 seconds)

Two separate thresholds are used intentionally. The higher open threshold prevents spurious session starts from momentary flow blips. The lower close threshold prevents session bouncing if flow dips briefly mid-shower (e.g. pressure fluctuation). Flow between the two thresholds is held — the session neither opens nor starts a close timer.

Volume is accumulated by the detector using trapezoidal integration of the `flow_rate_gpm` signal — no dependency on the HA volume sensor reset.

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

All signals are normalized to [0, 1] and summed. Maximum possible score is 1.0.

| Signal | Weight | Notes |
|---|---|---|
| Humidity slope (peak, span-3) | 30% | Primary attribution signal — rate of change over 3-reading lookback, normalized by `HUMIDITY_SLOPE_NORM` |
| Humidity delta above 12h avg (peak) | 25% | Confirms elevated state relative to dynamic baseline, normalized by `HUMIDITY_DELTA_NORM` |
| Temperature slope (peak, span-3) | 15% | Secondary confirmation signal, normalized by `TEMPERATURE_SLOPE_NORM` |
| Temperature delta above 12h avg (peak) | 15% | Secondary confirmation, normalized by `TEMPERATURE_DELTA_NORM` |
| Light (shower zone) seen during session | 5% | Positive signal, not required |
| Light (room) seen during session | 5% | Positive signal, not required |
| Fan seen during session | 5% | Positive signal, not required |

Peak values are tracked per session and only move upward — a single strong reading contributes even if later readings level off. All norm constants are configurable in `.env` for post-deployment tuning.

**Signals considered but not implemented:**
- *Water meter slope* — shower flow settles to a steady state quickly after opening; slope provides minimal additional signal
- *Competing bathroom suppression* — adds a bonus when the winner's slope significantly exceeds others; deferred until real attribution errors justify the complexity

### Slope Calculation

Slope is calculated as rise-over-run between the current reading and the reading 3 positions back in the sensor history deque (span-3 lookback). The history deque is trimmed to a 3-minute window, so span-3 represents the 3 most recent readings within that window. This smooths single outlier readings without requiring a full windowed regression.

### Attribution States

Attribution is computed once when the session closes. The detector picks the highest-scoring bathroom and assigns one of three states:

- **CONFIRMED** — winning score ≥ `ATTRIBUTION_CONFIRMED_THRESHOLD` (default: 0.6). High confidence.
- **ATTRIBUTED** — winning score ≥ `ATTRIBUTION_MIN_THRESHOLD` (default: 0.3) but below the confirmed threshold. Moderate confidence.
- **FALLBACK** — no bathroom cleared the minimum threshold. Session attributed to the first bathroom in `BATHROOM_PRIORITY_ORDER` that has any score.

Real-time score visibility during an active session is provided by the `home.shower_scores` topic, emitted every `SCORE_EMIT_INTERVAL_SECONDS`. This is separate from the final attribution and intended for monitoring and model tuning.

---

## Kafka Topic Design

### `home.water`

Water meter readings. One message per HA `state_changed` event — each message contains a single field, either `flow_rate_gpm` or `volume_gallons`, never both.

```json
{ "timestamp": "2024-01-15T20:41:00.000Z", "flow_rate_gpm": 2.3 }
```
```json
{ "timestamp": "2024-01-15T20:41:01.000Z", "volume_gallons": 12.4 }
```

The detector uses only `flow_rate_gpm` for session open/close logic and volume integration. The `volume_gallons` field is available in the topic for downstream consumers (e.g. InfluxDB raw flow visualization).

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
  "attribution_state": "CONFIRMED",
  "started_at": "2024-01-15T20:41:00.000Z",
  "ended_at": "2024-01-15T21:00:00.000Z",
  "duration_seconds": 1140,
  "volume_gallons": 57.2,
  "confidence_score": 0.87,
  "scores": { "bath1": 0.87, "bath2": 0.12, "bath3": 0.05 },
  "cost_estimate": null
}
```

`scores` contains the final score for every bathroom at session close, not just the winner. `cost_estimate` is null here and populated by downstream cost rollup logic.

### `home.shower_scores`

Per-bathroom scores emitted every `SCORE_EMIT_INTERVAL_SECONDS` during an active session. Used for real-time monitoring and model tuning in Grafana.

```json
{
  "session_id": "uuid",
  "timestamp": "2024-01-15T20:43:00.000Z",
  "scores": { "bath1": 0.45, "bath2": 0.08, "bath3": 0.02 }
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
WATER_COST_PER_GALLON=0.018

# Session logic
MIN_FLOW_RATE_OPEN=0.5          # GPM threshold to open a session
MIN_FLOW_RATE_CLOSE=0.1         # GPM threshold to start the close timer (hysteresis)
FLOW_ZERO_TIMEOUT_SECONDS=10    # Seconds below MIN_FLOW_RATE_CLOSE before session closes
MIN_SHOWER_DURATION_SECONDS=180 # Sessions shorter than this are discarded

# Attribution fallback priority (comma-separated bathroom IDs, first = highest priority)
BATHROOM_PRIORITY_ORDER=bath1,bath2,bath3

# Attribution scoring thresholds
ATTRIBUTION_CONFIRMED_THRESHOLD=0.6
ATTRIBUTION_MIN_THRESHOLD=0.3

# Score normalization constants (tune against real session data)
HUMIDITY_SLOPE_NORM=0.05
HUMIDITY_DELTA_NORM=15.0
TEMPERATURE_SLOPE_NORM=0.02
TEMPERATURE_DELTA_NORM=5.0

# Score reporting interval (seconds) during active sessions
SCORE_EMIT_INTERVAL_SECONDS=30
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
| aiokafka directly over faust-streaming | faust-streaming dependency chain (aiokafka + kafka-python) is fragile on Python 3.11+; aiokafka standalone is stable |
| Confidence scoring over binary logic | One bathroom has no smart lights; scoring handles missing signals gracefully |
| Humidity slope over threshold crossing | Slope is a more reliable real-time signal; threshold crossing has too much lag and ambient interference |
| Attribution at session close, not real-time | Scores accumulate peak values throughout the session; final attribution is more accurate than a mid-session snapshot |
| Peak signal tracking | A single strong humidity slope reading early in the session remains the attribution signal even if humidity plateaus later |
| Two-threshold hysteresis (open/close) | Prevents session bouncing from momentary pressure fluctuations without requiring debounce logic |
| Temperature included in scoring | Adds 30% weight alongside humidity; validated in real sessions — both signals contribute |
| Span-3 slope lookback | Smooths single outlier sensor readings without adding windowed regression complexity |
| home.shower_scores topic | Provides real-time score visibility for Grafana tuning without changing the session-close payload |
| Output topic (`home.showers`) | Decouples detection from downstream consumers; consumers don't need to know detection logic |
| InfluxDB over TimescaleDB | Already running in homelab; purpose-built for time-series; avoids adding another database |
| cost-aggregator as InfluxDB Flux tasks | Avoids a separate running container; rollup logic lives where the data lives |
| Overlapping showers ignored | Rare edge case; first attribution wins; not worth the complexity to solve now |