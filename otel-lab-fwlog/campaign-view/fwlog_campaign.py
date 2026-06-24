"""
fwlog_campaign.py — Same as fwlog.py but groups ALL runs of the same
base campaign under ONE operation in App Insights.

Difference from fwlog.py:
    fwlog.py         → operation_Name = cmpgn_cd  (e.g. "cccs18001001-f14")
                       each run version = separate operation
    fwlog_campaign.py → operation_Name = base campaign (e.g. "cccs18001001")
                        f14, f15, f16 … all appear INSIDE the same operation

How base campaign is derived:
    cmpgn_cd  "cccs18001001-f14"  →  base  "cccs18001001"
    Pattern   strip last token after final hyphen that matches [a-z]+[0-9]+
    If no suffix found, use cmpgn_cd as-is.

App Insights output (End-to-end transaction view):
    cccs18001001  [══════════════════════════════════════════]  ← 1 operation
      ingestion   [██]                                          ← from fwlog-001 (f14)
      api_create       [████]
      transformation         [██████]                          ← from fwlog-002 (f14)
      kafka                         [████]                     ← from fwlog-003 (f14)
      ingestion   [██]                                          ← from fwlog-001 (f15)
      ...

Usage:
    python fwlog_campaign.py <CSV_FILE>
"""

import csv
import logging
import os
import re
import sys
import uuid
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Guard: always run via uv run python
# ---------------------------------------------------------------------------
if not os.environ.get("_FWLOG_UV_SPAWNED"):
    import subprocess
    _env = os.environ.copy()
    _env["_FWLOG_UV_SPAWNED"] = "1"
    result = subprocess.run(["uv", "run", "python"] + sys.argv, env=_env)
    sys.exit(result.returncode)

logging.basicConfig(level=logging.WARNING, format="%(message)s")
logging.getLogger("azure.monitor.opentelemetry.exporter.export._base").setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Load .env
# ---------------------------------------------------------------------------
def _load_dotenv() -> None:
    for d in (Path(__file__).parent,
              Path(__file__).parent.parent,
              Path(__file__).parent.parent.parent):
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
# operationMap: {base_campaign → operationId}
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
# Derive base campaign — strip version suffix (e.g. -f14, -e9, -hb1)
# ---------------------------------------------------------------------------
_SUFFIX_RE = re.compile(r"-[a-z]+\d+$", re.IGNORECASE)

def base_campaign(cmpgn_cd: str) -> str:
    """'cccs18001001-f14' → 'cccs18001001'  |  'cccs18001001' → 'cccs18001001'"""
    return _SUFFIX_RE.sub("", cmpgn_cd.strip())

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
def run(csv_path: Path, campaign_name: str = "Campaign") -> None:
    print(f"Python: {sys.executable}")

    # 1 — read CSV
    with csv_path.open(newline="") as f:
        records = list(csv.DictReader(f))
    if not records:
        sys.exit("No records in CSV.")

    # 2 — resolve operationId keyed by VERSIONED cmpgn_cd → each run gets its own trace
    op_map   = load_map()
    original = set(op_map)
    for r in records:
        key = r["cmpgn_cd"].strip()
        if key not in op_map:
            op_map[key] = uuid.uuid4().hex
    new_keys = set(op_map) - original
    if new_keys:
        save_map(op_map)
        print(f"operationMap: new → {sorted(new_keys)}")

    # 3 — group rows by VERSIONED cmpgn_cd (each campaign run = separate trace)
    groups: dict[str, list] = defaultdict(list)
    for r in records:
        groups[r["cmpgn_cd"].strip()].append(r)

    # 4 — configure OTel → App Insights
    conn = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING", "")
    if not conn:
        sys.exit("ERROR: APPLICATIONINSIGHTS_CONNECTION_STRING not set in .env")

    from azure.monitor.opentelemetry import configure_azure_monitor
    from opentelemetry import trace
    from opentelemetry.trace import (SpanKind, StatusCode,
                                     SpanContext, TraceFlags, NonRecordingSpan)
    from opentelemetry.sdk.resources import Resource

    configure_azure_monitor(
        connection_string=conn,
        resource=Resource.create({"service.name": csv_path.stem}),
        sampling_ratio=1.0,
    )
    tracer = trace.get_tracer("fwlog_campaign")

    # 5 — send spans: one trace per versioned cmpgn_cd, root span name = campaign_name
    total = 0
    for cmpgn_cd, rows in groups.items():
        op_id = op_map[cmpgn_cd]

        load_times = [to_ns(r.get("load_tms", "")) or time.time_ns() for r in rows]
        timings = []
        for r, lt in zip(rows, load_times):
            d = mins_to_ns(r.get("duration", "0"))
            timings.append((lt, lt + d))
        root_start = min(s for s, _ in timings)
        root_end   = max(e for _, e in timings)

        # trace_id from versioned cmpgn_cd operationId → fully isolated per run
        trace_id   = int(op_id[:32], 16)
        parent_sid = int(op_id[16:32], 16) & 0xFFFFFFFFFFFFFFFF
        parent_ctx = SpanContext(
            trace_id=trace_id, span_id=parent_sid,
            is_remote=True, trace_flags=TraceFlags(TraceFlags.SAMPLED),
        )
        remote_ctx = trace.set_span_in_context(NonRecordingSpan(parent_ctx))

        # root span name = campaign_name arg → operation_Name e.g. "Campaign-g5"
        # each versioned cmpgn_cd has its own unique trace (operationId)
        root = tracer.start_span(
            name=campaign_name,                 # ← e.g. "Campaign-g5"
            context=remote_ctx,
            kind=SpanKind.SERVER,
            attributes={
                "operationId":   op_id,
                "cmpgn_cd":      cmpgn_cd,
                "base_campaign": base_campaign(cmpgn_cd),
                "source_file":   csv_path.name,
            },
            start_time=root_start,
        )
        root.set_status(StatusCode.OK)
        root_ctx = trace.set_span_in_context(root)
        print(f"\n  [{campaign_name} / {cmpgn_cd}]  operationId={op_id}")

        # child spans — step rows
        for r, (s, e) in zip(rows, timings):
            process = r.get("process", "").strip()
            attrs   = {k: v.strip() for k, v in r.items() if v and v.strip()}
            if "duration" in attrs:
                attrs["duration_min"] = attrs.pop("duration")
            attrs["operationId"]   = op_id
            attrs["base_campaign"] = base_campaign(cmpgn_cd)

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
                  f"dur={r.get('duration','').strip()}min  "
                  f"({cmpgn_cd})")

        root.end(end_time=root_end)
        total += 1

    # 6 — flush + shutdown
    from opentelemetry import metrics as otel_metrics
    tp = trace.get_tracer_provider()
    mp = otel_metrics.get_meter_provider()
    if hasattr(tp, "force_flush"): tp.force_flush(timeout_millis=15000)
    if hasattr(mp, "force_flush"): mp.force_flush(timeout_millis=15000)
    tp.shutdown()
    if hasattr(mp, "shutdown"): mp.shutdown()

    print(f"\nDone: {total} span(s) from '{csv_path.name}' — "
          f"{len(groups)} campaign(s)")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(f"Usage: python {Path(__file__).name} <CSV_FILE> [CAMPAIGN_NAME]")
    p = Path(sys.argv[1])
    if not p.is_absolute():
        p = SCRIPT_DIR / p
    name = sys.argv[2] if len(sys.argv) >= 3 else "Campaign"
    run(p, name)
