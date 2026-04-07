# Claude Code Prompt: inference-logger

Build a Python library called `inference-logger` — a unified inference logging framework for batch and real-time ML models. It writes prediction records to Delta Lake tables.

## Core Design Decisions (already made — follow these exactly)

### Architecture

- Pluggable sinks (Delta, Kafka, HTTP API), shared core engine, multiple thin interface adapters
- Delta tables are the primary sink and the single source of truth at model serving time
- Each model gets its own Delta table at `{base_path}/{model_id}/`, partitioned by `date` (YYYY-MM-DD) and `hour` (00-23)
- The `ground_truth_label` field starts null and is backfilled later via Delta MERGE (not part of this library — just include the column)

### Schema: PredictionRecord (Pydantic v2, Approach 1 — fixed core + flexible fields)

Core fields (required): `request_id` (auto UUID), `model_id`, `model_version`, `timestamp` (auto UTC), `features` (dict[str, Any]), `prediction_raw` (float | int | dict | list)
Decision fields (optional): `prediction_label`, `threshold_applied`
Operational fields (optional): `latency_ms` (ge=0), `feature_metadata`, `request_source`, `model_stage`
Diagnostic fields (optional): `explanation`, `segment` (dict[str, str]), `feature_drift_flags`
Catch-all: `metadata` (dict[str, Any])
Label: `ground_truth_label` (nullable string, backfilled later)
Partition helpers: `date_partition` and `hour_partition` properties, `to_flat_dict()` method that adds `date` and `hour` columns and JSON-serializes dict/list fields
Validators: non-empty model_id/model_version, non-empty features dict, extra=“forbid”

### Interfaces (4 adapters, all delegate to core engine)

1. **SingleRecordLogger** — `.log(**kwargs)` for explicit calls
1. **DecoratorLogger** — `@track()` decorator for real-time. Auto-captures features (from named param), prediction (return value), latency. Supports: `threshold` + `positive_label`/`negative_label`, `label_fn`, `prediction_extractor`, `request_id_param`, `request_source`, `_log_ctx` injection for extra fields. Auto-detects sync vs async functions. Does NOT mutate caller’s kwargs dict.
1. **PandasLogger** — `.log_dataframe()` for batch. Uses vectorized `to_dict("records")` + `tolist()` (NOT iterrows). Returns original df for `.pipe()` compatibility. Handles NaN prediction labels correctly (None, not “nan”). Validates column presence.
1. **PySparkLogger** — `.log_spark_dataframe()` for distributed batch. Writes `.format("delta")` with `partitionBy("date", "hour")` and `mergeSchema=true`. `output_path` is optional — inferred from configured sink + model_id. Packs features into a map column.

### Core Engine (InferenceLogEngine)

- Thread-safe buffer with configurable max_size and flush_interval_seconds
- Background flush thread (daemon) for time-based flushing
- Lock is held ONLY during buffer copy+clear — all I/O happens outside the lock
- Exponential backoff retry on sink failures
- Batch interfaces (Pandas, PySpark) bypass the buffer and write directly to sinks
- `shutdown()` is idempotent (guarded by `_shutdown_called` flag)
- Registered with `atexit` for automatic cleanup

### Sinks (5 total)

1. **LocalDeltaSink** — writes via `deltalake.write_deltalake()` with `mode="append"`, `partition_by=["date", "hour"]`, `schema_mode="merge"`. Groups records by model_id, writes each group to `{base_path}/{model_id}/`. Casts PyArrow null-typed columns to string before write (Delta doesn’t support Null type).
1. **S3DeltaSink** — same as local but path is `s3://{bucket}/{prefix}/{model_id}`, passes `storage_options` to deltalake.
1. **KafkaSink** — publishes each record as JSON to a Kafka topic via confluent-kafka Producer.
1. **APISink** — POSTs to HTTP endpoint. Batch mode (JSON array per flush) or individual mode (one POST per record). Auth: API key header, bearer token, or custom headers. Optional gzip compression. Uses `requests.Session` for connection pooling. Uses `model_dump(mode="json")` not double-serialization.
1. Factory function `create_sink(config)` maps config types to sink classes.

### Config (Pydantic v2 models)

- `LocalDeltaSinkConfig(base_path="./inference_logs")`
- `S3DeltaSinkConfig(bucket, prefix, storage_options)`
- `KafkaSinkConfig(bootstrap_servers, topic, producer_kwargs)`
- `APISinkConfig(url, batch_mode, auth_type, api_key, bearer_token, custom_headers, timeout_seconds, compress)`
- `BufferConfig(max_size=1000, flush_interval_seconds=30, max_retries=3, retry_backoff_seconds=1.0)`
- `InferenceLoggerConfig(sinks, buffer, default_model_id, default_model_version, default_request_source, raise_on_validation_error, enable_logging)`

### Facade (InferenceLogger)

- Single class that exposes all interfaces with FULL typed signatures (no **kwargs passthrough — IDE autocomplete must work)
- Facade uses `model_version` consistently (the internal DecoratorLogger.track uses `version` but the facade normalizes)
- `__repr__` shows sink names, buffer_size, logged count, failed count, enabled status
- Properties: `records_logged`, `records_failed`, `buffer_size`, `engine`

### Numpy/Pandas sanitization

- Cache `import numpy` and `import pandas` at module level (not per-call)
- Handle np.integer, np.floating (NaN→None), np.ndarray, np.bool_, pd.isna

## Dependencies

- Core: pydantic>=2.0
- Delta sinks: deltalake>=0.18.0, pyarrow>=14.0
- Kafka: confluent-kafka>=2.0
- API: requests>=2.28
- Pandas interface: pandas>=2.0, deltalake, pyarrow
- PySpark interface: pyspark>=3.4, delta-spark>=3.0

## Tests

Write comprehensive pytest tests covering:

- Schema validation (valid records, multiclass, ranking, empty model_id, empty features, extra fields, negative latency, date/hour partitions, ground_truth_label, to_flat_dict)
- Engine (log+flush, auto-flush on buffer full, validation failure silent/raises, global defaults, Delta table written with correct partitions and columns, disabled logging)
- Decorator (sync, async, threshold with custom labels, prediction_extractor, _log_ctx, exception propagation)
- Pandas (log_dataframe, pipe compatibility, missing column errors, NaN label handling)
- Delta sink (per-model tables, mixed models separate tables, empty records noop, schema evolution on append)
- API sink (batch mode single POST, gzip compression)
- End-to-end (full roundtrip through multiple interfaces, concurrent logging with 4 threads, PySpark output path inference from local and S3 configs)

## Project Structure

```
inference_logger/
├── pyproject.toml
├── README.md
├── examples.py
├── inference_logger/
│   ├── __init__.py          # InferenceLogger facade + exports
│   ├── schema.py            # PredictionRecord
│   ├── config.py            # All config models
│   ├── engine.py            # InferenceLogEngine
│   ├── interfaces/
│   │   └── __init__.py      # All 4 interface adapters
│   └── sinks/
│       └── __init__.py      # All sinks + factory
└── tests/
    └── test_inference_logger.py
```

Build everything, run the tests, make sure they all pass, then package.