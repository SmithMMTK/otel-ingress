"""
fwlog.py — Ship CSV rows to Application Insights as grouped traces.

Usage:
    uv run python fwlog.py <CSV_FILE>

CSV schema:  process, pos_dt, load_tms, cmpgn_cd, count_lead, duration

App Insights output (End-to-end transaction view):
    cccs18001001-4  [══════════════════════════]   ← root  (cmpgn_cd)
      ingestion     [██]                            ← child (process, @ load_tms)
      api_create         [████]
      api_create               [████]

    cccs18001002-4  [══]
      ingestion     [██]
"""

import csv
import logging
import os
import sys
import uuid
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Guard: always run via `uv run python` for correct venv + flush behavior.
# Uses a sentinel env var to prevent infinite re-exec loop.
# ---------------------------------------------------------------------------
if not os.environ.get("_FWLOG_UV_SPAWNED"):
    import subprocess
    _env = os.environ.copy()
    _env["_FWLOG_UV_SPAWNED"] = "1"
    result = subprocess.run(["uv", "run", "python"] + sys.argv, env=_env)
    sys.exit(result.returncode)

# Show transmission results so user knows data was accepted
logging.basicConfig(level=logging.WARNING, format="%(message)s")
logging.getLogger("azure.monitor.opentelemetry.exporter.export._base").setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Load .env (walk up from script dir to repo root)
# ---------------------------------------------------------------------------
def _load_dotenv() -> None:
    for d in (Path(__file__).parent, Path(__file__).parent.parent):
        f = d / ".env"
        if f.exists():
            for line in f.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k = k.strip(); v = v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
            return

_load_dotenv()

SCRIPT_DIR    = Path(__file__).parent
OPERATION_MAP = SCRIPT_DIR / "operationMap.csv"

# ---------------------------------------------------------------------------
# operationMap: {cmpgn_cd → operationId}
# ---------------------------------------------------------------------------
def load_map() -> dict[str, str]:
    if not OPERATION_MAP.exists():
        return {}
    with OPERATION_MAP.open(newline="") as f:
        return {r["cmpgn_cd"].strip(): r["operationId"].strip()
                for r in csv.DictReader(f)}

def save_map(m: dict[str, str]) -> None:
    with OPERATION_MAP.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["cmpgn_cd", "operationId"])
        for k, v in sorted(m.items()):
            w.writerow([k, v])

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_NS = 1_000_000_000

def to_ns(iso: str) -> int | None:
    try:
        dt = datetime.fromisoformat(iso.strip())
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * _NS)
    except Exception:
        return None

def mins_to_ns(minutes: str) -> int:
    try:
        return int(float(minutes.strip()) * 60 * _NS)
    except Exception:
        return 0

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(csv_path: Path) -> None:
    print(f"Python: {sys.executable}")

    # 1 — read CSV
    with csv_path.open(newline="") as f:
        records = list(csv.DictReader(f))
    if not records:
        sys.exit("No records in CSV.")

    # 2 — resolve operationId for every cmpgn_cd
    op_map   = load_map()
    original = set(op_map)
    for r in records:
        code = r["cmpgn_cd"].strip()
        if code not in op_map:
            op_map[code] = uuid.uuid4().hex          # brand-new 32-char hex
    new_codes = set(op_map) - original
    if new_codes:
        save_map(op_map)
        print(f"operationMap: new → {sorted(new_codes)}")

    # 3 — group rows by cmpgn_cd
    groups: dict[str, list] = defaultdict(list)
    for r in records:
        groups[r["cmpgn_cd"].strip()].append(r)

    # 4 — configure OTel → App Insights
    conn = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING", "")
    if not conn:
        sys.exit("ERROR: APPLICATIONINSIGHTS_CONNECTION_STRING not set in .env")

    from azure.monitor.opentelemetry import configure_azure_monitor
    from opentelemetry import trace
    from opentelemetry.trace import SpanKind, StatusCode
    from opentelemetry.sdk.resources import Resource

    configure_azure_monitor(
        connection_string=conn,
        resource=Resource.create({"service.name": csv_path.stem}),
        sampling_ratio=1.0,                          # no sampling — send everything
    )
    tracer = trace.get_tracer("fwlog")

    # 5 — send spans
    total = 0
    for cmpgn_cd, rows in groups.items():
        op_id = op_map[cmpgn_cd]

        # root span timing: use load_tms directly as span timestamps
        load_times = [to_ns(r.get("load_tms", "")) or time.time_ns() for r in rows]

        timings = []
        for r, lt in zip(rows, load_times):
            d = mins_to_ns(r.get("duration", "0"))
            timings.append((lt, lt + d))
        root_start = min(s for s, _ in timings)
        root_end   = max(e for _, e in timings)

        # root span: use operationId as trace_id so all CSV files with same
        # cmpgn_cd share the same trace in App Insights E2E view
        from opentelemetry.trace import SpanContext, TraceFlags, NonRecordingSpan
        trace_id   = int(op_id[:32], 16)                 # 128-bit trace id from operationId
        parent_sid = int(op_id[16:32], 16) & 0xFFFFFFFFFFFFFFFF  # 64-bit span id
        parent_ctx = SpanContext(
            trace_id=trace_id,
            span_id=parent_sid,
            is_remote=True,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
        )
        remote_ctx = trace.set_span_in_context(NonRecordingSpan(parent_ctx))

        # root span: SERVER → sets operation_Name = cmpgn_cd in App Insights
        root = tracer.start_span(
            name=cmpgn_cd,
            context=remote_ctx,
            kind=SpanKind.SERVER,
            attributes={"operationId": op_id, "source_file": csv_path.name},
            start_time=root_start,
        )
        root.set_status(StatusCode.OK)
        root_ctx = trace.set_span_in_context(root)
        print(f"\n  [{cmpgn_cd}]  operationId={op_id}")

        # child spans: name = process  →  bars inside the group
        for r, (s, e) in zip(rows, timings):
            process = r.get("process", "").strip()
            attrs   = {k: v.strip() for k, v in r.items() if v and v.strip()}
            # Rename 'duration' → 'duration_min' so App Insights doesn't
            # treat it as the telemetry duration and override span timing.
            if "duration" in attrs:
                attrs["duration_min"] = attrs.pop("duration")
            attrs["operationId"] = op_id

            child = tracer.start_span(
                name=process,
                context=root_ctx,
                kind=SpanKind.INTERNAL,
                attributes=attrs,
                start_time=s,
            )
            child.set_status(StatusCode.OK)
            child.end(end_time=e)
            total += 1
            print(f"    → {process:15}  {r.get('load_tms','').strip()}  "
                  f"dur={r.get('duration','').strip()}min")

        root.end(end_time=root_end)
        total += 1

    # 6 — flush all three providers (trace + metrics + logs) then shutdown
    # force_flush blocks until export completes (up to timeout)
    from opentelemetry import metrics as otel_metrics
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.metrics import MeterProvider

    tp = trace.get_tracer_provider()
    mp = otel_metrics.get_meter_provider()

    if hasattr(tp, "force_flush"):
        tp.force_flush(timeout_millis=15000)
    if hasattr(mp, "force_flush"):
        mp.force_flush(timeout_millis=15000)

    tp.shutdown()
    if hasattr(mp, "shutdown"):
        mp.shutdown()

    print(f"\nDone: {total} span(s) from '{csv_path.name}' — "
          f"{len(groups)} operation(s)")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(f"Usage: python {Path(__file__).name} <CSV_FILE>")
    p = Path(sys.argv[1])
    if not p.is_absolute():
        p = SCRIPT_DIR / p
    run(p)
