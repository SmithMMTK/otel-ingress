# AGENTS.md — AI Instruction for campaign-view

This file tells AI assistants (GitHub Copilot, Cursor, Claude, etc.)
everything they need to understand, run, and extend this project.

---

## What this project does

Reads pipeline job records from CSV files and ships them to
**Azure Application Insights** as OpenTelemetry traces.

The goal is to make pipeline runs (ingestion → transformation → kafka)
**visible in App Insights** with full E2E transaction timeline,
custom properties, and KQL-queryable telemetry.

---

## Folder layout

```
campaign-view/
├── fwlog_campaign.py     ← main script (only file you need to understand)
├── operationMap.csv      ← persistent cmpgn_cd → operationId mapping
├── fwlog-001.csv         ← ingestion + api_create records
├── fwlog-002.csv         ← transformation records
├── fwlog-003.csv         ← kafka records
├── README.md             ← human-readable guide
└── AGENTS.md             ← this file
```

Parent folder `otel-lab-fwlog/` contains:
- `fwlog.py` — original script (operation_Name = versioned cmpgn_cd)
- `.env` — secrets (gitignored), must contain `APPLICATIONINSIGHTS_CONNECTION_STRING`

---

## How to run

```bash
# requires: uv installed, .env with APPLICATIONINSIGHTS_CONNECTION_STRING
python fwlog_campaign.py fwlog-001.csv Campaign-g8
python fwlog_campaign.py fwlog-002.csv Campaign-g8
python fwlog_campaign.py fwlog-003.csv Campaign-g8
```

Script auto-relaunches via `uv run python` if not already in uv env
(controlled by `_FWLOG_UV_SPAWNED` sentinel env var).

---

## CSV schema (source of truth)

```
process,pos_dt,load_tms,cmpgn_cd,count_lead,duration
```

| Column | Type | Role |
|---|---|---|
| `process` | string | step name → OTel child span name |
| `pos_dt` | date `YYYY-MM-DD` | business date → custom attribute |
| `load_tms` | ISO 8601 datetime with tz | `start_time` of span |
| `cmpgn_cd` | string e.g. `cccs18001001-g8` | operationMap key + custom attribute |
| `count_lead` | int | business metric → custom attribute |
| `duration` | float (minutes) | `end_time = start_time + duration_min * 60s` |

All CSV columns are automatically forwarded as `customDimensions` in App Insights.
Adding a new column = adding a new custom property, no code change needed.

---

## operationMap.csv — cross-file trace correlation

```csv
cmpgn_cd,operationId
cccs18001001-g8,c2d8514fae40462b9508a7169ed4247a
cccs18001002-g8,2cfb2f3e8b41492781889cf85b6fe425
```

**Rules:**
- Key = full versioned `cmpgn_cd` (e.g. `cccs18001001-g8`)
- Value = 32-char hex UUID (no dashes) used as `trace_id`
- If key not found → auto-create new UUID → append to file
- Same key across fwlog-001/002/003 → all spans land in same trace
- Each new version suffix (g8 vs g9) = new key = new isolated trace

**Never edit this file manually** unless resetting a campaign's trace identity.

---

## OTel span model

```
[Remote SpanContext]           ← synthetic parent, trace_id derived from operationId
    │
    └── <CAMPAIGN_NAME>        SpanKind.SERVER   ← root span
            │                                      → sets operation_Name in App Insights
            │                                      → stored in `requests` table
            ├── ingestion      SpanKind.INTERNAL  ← child span per CSV row
            ├── api_create     SpanKind.INTERNAL    → stored in `dependencies` table
            ├── transformation SpanKind.INTERNAL    → inherits operation_Name + operation_Id
            └── kafka          SpanKind.INTERNAL
```

**Key design decisions:**
- `SpanKind.SERVER` → App Insights `requests` table → sets `operation_Name`
- `SpanKind.INTERNAL` → App Insights `dependencies` table → queryable by step name
- `trace_id = int(operationId[:32], 16)` → deterministic, reproducible
- `force_flush()` + `shutdown()` at end → mandatory, prevents span loss

---

## App Insights data model

After ingestion, query in App Insights → Logs:

```kql
-- Root spans (one per cmpgn_cd per run)
requests
| where operation_Name startswith "Campaign"
| project timestamp, operation_Name, operation_Id, duration

-- Step spans with custom properties
dependencies
| where operation_Name startswith "Campaign"
| extend cmpgn_cd   = tostring(customDimensions["cmpgn_cd"])
| extend count_lead = toint(customDimensions["count_lead"])
| extend dur_min    = duration / 60000.0
| project timestamp, name, cmpgn_cd, dur_min, count_lead, operation_Id
```

**Timing note:** `duration` in App Insights = real milliseconds (not timespan).
Always convert with `/ 60000.0` to get minutes.

**Cold path:** Spans with timestamps older than ~5 minutes may take
5–45 minutes to appear in App Insights (cold ingestion path).
Use "Last 7 days" time range when querying historical data.

---

## Azure Monitor Batching (important for large CSV)

```
spans → BatchSpanProcessor queue
              │
         max_queue_size = 2048   ← default, spans DROPPED if exceeded
         max_export_batch_size = 512  ← one HTTP POST per 512 spans
              │
         HTTP POST to App Insights endpoint
```

For CSV files with > 500 rows, override defaults:
```python
# After configure_azure_monitor(), replace the BatchSpanProcessor:
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from azure.monitor.opentelemetry.exporter import AzureMonitorTraceExporter

exporter = AzureMonitorTraceExporter(connection_string=conn)
processor = BatchSpanProcessor(
    exporter,
    max_queue_size=10000,
    max_export_batch_size=512,
    schedule_delay_millis=5000,
)
trace.get_tracer_provider().add_span_processor(processor)
```

---

## How to extend

### Add a new pipeline step
Add rows to the appropriate CSV with new `process` name.
No code change needed — the script reads all rows generically.

```csv
validation,2026-06-24,2026-06-24T08:00:00+07:00,cccs18001001-g9,100,5
```

### Add a new CSV file (e.g. fwlog-004.csv for a new step group)
Same schema, different `process` values.
Run with the same CAMPAIGN_NAME to keep spans in the same operation.

### Add error/failure scenario
```python
# In the child span creation block:
if r.get("status") == "failed":
    child.set_status(StatusCode.ERROR, r.get("error_msg", "unknown"))
    child.set_attribute("error", True)
else:
    child.set_status(StatusCode.OK)
```

### Change what operation_Name shows
Pass different CAMPAIGN_NAME arg:
```bash
python fwlog_campaign.py fwlog-001.csv "MyPipeline-2026Q3"
```

### Replace CSV with real data source (Spark, DB, API)
Swap out the CSV reader block (lines ~128–133 in fwlog_campaign.py):
```python
# Replace:
with csv_path.open(newline="") as f:
    records = list(csv.DictReader(f))

# With e.g. Spark DataFrame:
records = [row.asDict() for row in spark_df.collect()]

# Or database cursor:
records = [dict(zip([d[0] for d in cursor.description], row))
           for row in cursor.fetchall()]
```
The rest of the script works unchanged as long as records have the same keys.

### Add metrics (not just traces)
```python
from opentelemetry import metrics
meter = metrics.get_meter("fwlog_campaign")
counter = meter.create_counter("pipeline.records_processed")
counter.add(len(records), {"cmpgn_cd": cmpgn_cd})
```

---

## Environment setup

```bash
# Install dependencies
uv sync  # reads pyproject.toml from parent otel-lab-fwlog/

# .env must contain (copy from parent folder or create here):
APPLICATIONINSIGHTS_CONNECTION_STRING=InstrumentationKey=...;IngestionEndpoint=...

# Verify by running dry (will fail gracefully with clear error if missing):
python fwlog_campaign.py fwlog-001.csv TestRun
```

---

## Conventions to follow when extending

1. **Never call `trace.set_tracer_provider()` directly** — use `configure_azure_monitor()`
2. **Always call `force_flush()` + `shutdown()`** at end of script — never skip
3. **Keep `operationMap.csv` as source of truth** for trace identity — don't hardcode UUIDs
4. **Use `SpanKind.SERVER` only for root span** — child steps use `SpanKind.INTERNAL`
5. **Rename `duration` attribute to `duration_min`** in attrs dict — App Insights intercepts the raw `duration` key and overrides span timing
6. **Historical timestamps are OK** — App Insights accepts backdated spans; use "Last 7 days" to query
7. **Don't break CSV schema** — existing columns are relied on by KQL workbook queries in `workbook-fwlog.json`
