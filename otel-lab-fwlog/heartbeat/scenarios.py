"""
scenarios.py — Run 3 heartbeat scenarios to demonstrate monitoring patterns.

Usage:
    python scenarios.py <base_cmpgn_cd>

    e.g.: python scenarios.py cccs18001001-hb1

Scenarios:
    S1 (success)    : job completes normally within SLA
    S2 (long)       : job completes but exceeds SLA — alert should fire during run
    S3 (timeout)    : job starts, sends a few heartbeats, then DIES (no job.complete)
                      → KQL detects missing heartbeat + no completion

Run S1 and S2 sequentially, S3 in background then exit abruptly.
Check App Insights after ~5 min with kql-heartbeat.md queries.
"""

import os
import sys
import time
import subprocess
from pathlib import Path

if not os.environ.get("_FWLOG_UV_SPAWNED"):
    _env = os.environ.copy()
    _env["_FWLOG_UV_SPAWNED"] = "1"
    result = subprocess.run(["uv", "run", "python"] + sys.argv, env=_env)
    sys.exit(result.returncode)

SCRIPT_DIR = Path(__file__).parent

def _run(cmpgn_cd: str, interval: int, duration: int, fail: bool = False,
         label: str = "", abrupt_exit_after: int = 0) -> None:
    """Run heartbeat_demo.py. If abrupt_exit_after > 0, kill it mid-run."""
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  cmpgn_cd={cmpgn_cd}  interval={interval}s  duration={duration}s")
    print(f"{'='*60}")

    env = os.environ.copy()
    env["_FWLOG_UV_SPAWNED"] = "1"

    cmd = [sys.executable, str(SCRIPT_DIR / "heartbeat_demo.py"),
           cmpgn_cd,
           "--interval", str(interval),
           "--duration", str(duration)]
    if fail:
        cmd.append("--fail")

    if abrupt_exit_after > 0:
        # Start process, wait N seconds, then KILL it (simulates crash/timeout)
        proc = subprocess.Popen(cmd, env=env)
        print(f"  [PID {proc.pid}] Running... will kill after {abrupt_exit_after}s to simulate crash")
        time.sleep(abrupt_exit_after)
        proc.kill()
        proc.wait()
        print(f"  💀 Process killed — no job.complete will be sent")
        print(f"  → KQL should detect: heartbeat stopped, job never completed")
    else:
        subprocess.run(cmd, env=env)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(f"Usage: python {Path(__file__).name} <base_cmpgn_cd>\n"
                 f"  e.g.: python {Path(__file__).name} cccs18001001-hb1")

    base = sys.argv[1]
    s1 = f"{base}-s1"   # success
    s2 = f"{base}-s2"   # long (exceeds SLA)
    s3 = f"{base}-s3"   # timeout / crash (no completion)

    print(f"""
Heartbeat Demo — 3 Scenarios
─────────────────────────────────────────────────────────────
S1 [{s1}]
   Normal job — completes in 60s (within SLA)
   Expected: job.start → 2x heartbeat → job.complete ✅

S2 [{s2}]
   Long job — completes in 150s (exceeds 90s SLA)
   Expected: job.start → 5x heartbeat → job.complete ⚠️
   Alert fires because gap between start and complete > SLA

S3 [{s3}]
   Crashed job — starts, sends 2 heartbeats, then DIES (no complete)
   Expected: job.start → 2x heartbeat → [silence] 💀
   KQL detects: last heartbeat > interval*3 minutes ago, no completion
─────────────────────────────────────────────────────────────
""")

    # S1 — success (60s, beat every 20s → ~2 beats)
    _run(s1, interval=20, duration=60,
         label="S1 · SUCCESS — normal job completes within SLA")

    # S2 — long (150s, beat every 20s → ~7 beats, SLA=90s)
    _run(s2, interval=20, duration=150,
         label="S2 · LONG — job completes but exceeds SLA threshold")

    # S3 — crash after 50s (2 beats @ 20s each, then killed)
    _run(s3, interval=20, duration=300,
         abrupt_exit_after=50,
         label="S3 · TIMEOUT/CRASH — job starts then dies, no completion span")

    print(f"""
{'='*60}
All 3 scenarios sent to App Insights.
Wait 2-5 minutes then run queries from kql-heartbeat.md:

  • Query A — see all heartbeat events per scenario
  • Query B — detect S3 (missing heartbeat + no completion)
  • Query C — detect S2 (completed but exceeded SLA)

Look for cmpgn_cd prefix: {base}
{'='*60}
""")
