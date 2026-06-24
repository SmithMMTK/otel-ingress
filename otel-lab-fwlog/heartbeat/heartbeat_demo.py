"""
heartbeat_demo.py — Simulate a long-running campaign job with heartbeat spans.

Usage:
    python heartbeat_demo.py <cmpgn_cd> [--interval 30] [--duration 120] [--fail]

What it sends to App Insights:
    job.start      → SERVER span  (marks job began)
    job.heartbeat  → INTERNAL span every --interval seconds (proves job is alive)
    job.complete   → INTERNAL span (marks job finished successfully)
    job.failed     → INTERNAL span (marks job failed, if --fail is passed)

KQL to detect stale (missing heartbeat):
    See kql-heartbeat.md in this folder.
"""

import csv
import os
import sys
import time
import uuid
import threading
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

import logging
logging.basicConfig(level=logging.WARNING, format="%(message)s")
logging.getLogger("azure.monitor.opentelemetry.exporter.export._base").setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Load .env (walk up from script dir to repo root)
# ---------------------------------------------------------------------------
def _load_dotenv() -> None:
    for d in (Path(__file__).parent, Path(__file__).parent.parent, Path(__file__).parent.parent.parent):
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
OPERATION_MAP = SCRIPT_DIR.parent / "operationMap.csv"   # share with fwlog

# ---------------------------------------------------------------------------
# operationMap helpers (shared with fwlog)
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
# Main
# ---------------------------------------------------------------------------
def run(cmpgn_cd: str, interval_sec: int, total_sec: int, fail: bool) -> None:
    # 1 — resolve operationId (share with fwlog pipeline)
    op_map  = load_map()
    if cmpgn_cd not in op_map:
        op_map[cmpgn_cd] = uuid.uuid4().hex
        save_map(op_map)
        print(f"operationMap: new → {cmpgn_cd}")
    op_id = op_map[cmpgn_cd]
    print(f"  cmpgn_cd    : {cmpgn_cd}")
    print(f"  operationId : {op_id}")
    print(f"  interval    : {interval_sec}s  |  total : {total_sec}s  |  fail={fail}")

    # 2 — configure OTel
    conn = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING", "")
    if not conn:
        sys.exit("ERROR: APPLICATIONINSIGHTS_CONNECTION_STRING not set")

    from azure.monitor.opentelemetry import configure_azure_monitor
    from opentelemetry import trace
    from opentelemetry.trace import SpanKind, StatusCode, SpanContext, TraceFlags, NonRecordingSpan

    configure_azure_monitor(
        connection_string=conn,
        sampling_ratio=1.0,
    )
    tracer  = trace.get_tracer("heartbeat")
    _NS     = 1_000_000_000

    # derive shared trace_id from operationId (same logic as fwlog.py)
    trace_id   = int(op_id[:32], 16)
    parent_sid = int(op_id[16:32], 16) & 0xFFFFFFFFFFFFFFFF
    parent_ctx = SpanContext(
        trace_id=trace_id, span_id=parent_sid,
        is_remote=True, trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )
    remote_ctx = trace.set_span_in_context(NonRecordingSpan(parent_ctx))

    # 3 — job.start (SERVER — shows as root in E2E view)
    job_start_ns = time.time_ns()
    root = tracer.start_span(
        name=cmpgn_cd,
        context=remote_ctx,
        kind=SpanKind.SERVER,
        attributes={"operationId": op_id, "event": "job.start", "cmpgn_cd": cmpgn_cd},
        start_time=job_start_ns,
    )
    root.set_status(StatusCode.OK)
    root_ctx = trace.set_span_in_context(root)
    print(f"\n▶  job.start")

    # 4 — heartbeat loop (background thread flushes each beat immediately)
    stop_event   = threading.Event()
    beat_count   = [0]
    flush_lock   = threading.Lock()

    def _beat():
        while not stop_event.wait(interval_sec):
            beat_count[0] += 1
            elapsed = (time.time_ns() - job_start_ns) // _NS
            beat = tracer.start_span(
                name="job.heartbeat",
                context=root_ctx,
                kind=SpanKind.INTERNAL,
                attributes={
                    "operationId": op_id,
                    "cmpgn_cd":    cmpgn_cd,
                    "beat_number": beat_count[0],
                    "elapsed_sec": elapsed,
                },
            )
            beat.set_status(StatusCode.OK)
            beat.end()
            with flush_lock:
                tp = trace.get_tracer_provider()
                if hasattr(tp, "force_flush"):
                    tp.force_flush(timeout_millis=5000)
            print(f"  💓 heartbeat #{beat_count[0]}  (elapsed {elapsed}s)")

    t = threading.Thread(target=_beat, daemon=True)
    t.start()

    # 5 — simulate work
    try:
        time.sleep(total_sec)
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        fail = True
    finally:
        stop_event.set()
        t.join(timeout=interval_sec + 2)

    # 6 — job.complete or job.failed
    job_end_ns = time.time_ns()
    event_name = "job.failed" if fail else "job.complete"
    status     = StatusCode.ERROR if fail else StatusCode.OK

    final = tracer.start_span(
        name=event_name,
        context=root_ctx,
        kind=SpanKind.INTERNAL,
        attributes={
            "operationId":  op_id,
            "cmpgn_cd":     cmpgn_cd,
            "total_beats":  beat_count[0],
            "elapsed_sec":  (job_end_ns - job_start_ns) // _NS,
        },
        start_time=job_end_ns,
    )
    final.set_status(status)
    final.end(end_time=job_end_ns + 1)
    root.end(end_time=job_end_ns + 2)
    print(f"\n{'❌' if fail else '✅'}  {event_name}  (beats={beat_count[0]})")

    # 7 — flush all providers
    from opentelemetry import metrics as otel_metrics
    tp = trace.get_tracer_provider()
    mp = otel_metrics.get_meter_provider()
    if hasattr(tp, "force_flush"): tp.force_flush(timeout_millis=15000)
    if hasattr(mp, "force_flush"): mp.force_flush(timeout_millis=15000)
    tp.shutdown()
    if hasattr(mp, "shutdown"): mp.shutdown()
    print("Done — spans flushed to App Insights")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Heartbeat demo for campaign job monitoring")
    p.add_argument("cmpgn_cd",           help="Campaign code (e.g. cccs18001001-hb1)")
    p.add_argument("--interval", type=int, default=30,  help="Heartbeat interval seconds (default 30)")
    p.add_argument("--duration",  type=int, default=120, help="Total job duration seconds (default 120)")
    p.add_argument("--fail",      action="store_true",   help="Simulate job failure at end")
    args = p.parse_args()
    run(args.cmpgn_cd, args.interval, args.duration, args.fail)
