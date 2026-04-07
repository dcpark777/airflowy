# Claude Code Prompt: inference-logger (concept)

Build a Python library called `inference-logger` — a unified inference logging framework for batch and real-time ML models.

## Problem

Every ML monitoring system (drift detection, performance tracking, alerting) depends on structured inference logs. Without them, you can’t diagnose production issues. This library makes logging predictions a one-liner regardless of how a model serves — real-time API, Pandas batch job, or PySpark pipeline.

## Core Idea

One shared schema and core engine, multiple thin interface adapters for different calling patterns, pluggable storage sinks.

```
Interfaces (thin)              Core Engine (shared)           Sinks (pluggable)
─────────────────              ────────────────────           ──────────────────
@track() decorator   ─┐
.log() explicit call ─┤──→  validate → buffer → flush  ──→  Delta Lake (primary)
.log_dataframe()     ─┤       (schema)   (async)  (retry)    Kafka
.log_spark_dataframe()┘                                       HTTP API
```

## Key Requirements

- **Minimal integration effort.** A data scientist should be able to add logging to an existing model with 1-3 lines of code.
- **Works for both batch and real-time.** Real-time models need async buffering so logging doesn’t add latency. Batch models need DataFrame-native interfaces that don’t force row-by-row iteration.
- **One schema for all models.** A fixed set of core fields (model_id, features, prediction, timestamp) plus flexible fields that accept arbitrary structures — so binary classifiers, multi-class models, and ranking models all use the same schema.
- **Pluggable sinks.** Delta Lake is the primary storage (ACID writes, schema evolution, label backfill via MERGE). But also support Kafka and HTTP API endpoints for teams that need them.
- **Per-model datasets.** Each model gets its own Delta table, partitioned by date and hour. Independent retention, permissions, and monitoring configs.
- **Ground truth label backfill.** The schema includes a `ground_truth_label` column that starts null and gets filled in later via Delta MERGE when labels arrive (days/weeks later for fraud).
- **Thread-safe.** Multiple concurrent requests in a real-time serving environment must not corrupt the buffer or produce partial writes.
- **Configurable.** Sinks, buffer size, flush interval, retries, global defaults (model_id, version) — all configurable. Master switch to disable logging entirely for tests.

## Interfaces to Support

1. **Decorator** — `@logger.track(model_id=..., model_version=...)` wraps a predict function, auto-captures inputs, output, and latency. Works with sync and async functions.
1. **Explicit log** — `logger.log(model_id=..., features=..., prediction_raw=...)` for when the decorator doesn’t fit.
1. **Pandas DataFrame** — `logger.log_dataframe(df, model_id=..., feature_columns=..., prediction_column=...)` for batch scoring jobs. Should be performant on large DataFrames (no iterrows).
1. **PySpark DataFrame** — `logger.log_spark_dataframe(df, model_id=..., ...)` for distributed batch. Should write Delta format via Spark’s native writer, not collect to driver.

## Schema Fields to Include

Core (required): request_id, model_id, model_version, timestamp, features, prediction_raw
Decision (optional): prediction_label, threshold_applied
Operational (optional): latency_ms, feature_metadata, request_source, model_stage
Diagnostic (optional): explanation (SHAP values), segment (for sliced monitoring), feature_drift_flags
Label (backfilled): ground_truth_label
Partitions (auto-generated): date, hour

## Tech Stack

- Pydantic v2 for schema validation
- Delta Lake (`deltalake` Python library for non-Spark writes, `delta-spark` for PySpark)
- PyArrow for columnar data handling
- `requests` for HTTP API sink
- `confluent-kafka` for Kafka sink
- All heavy dependencies are optional extras — core only requires Pydantic

## What to Build

1. Schema model (PredictionRecord)
1. Config models (sink configs, buffer config, top-level config)
1. Core engine (validate, buffer, flush, retry, thread safety)
1. Sink implementations (Delta local, Delta S3, Kafka, HTTP API)
1. Interface adapters (decorator, explicit, Pandas, PySpark)
1. Unified facade class (InferenceLogger) that exposes all interfaces
1. Tests
1. README with quickstart and examples

Start by proposing the detailed design, then build it.