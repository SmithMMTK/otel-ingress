#!/usr/bin/env bash
# run-heartbeat.sh — Run 3 heartbeat scenarios (success / long / crash)
# Usage: ./run-heartbeat.sh [base_cmpgn_cd]
#   e.g: ./run-heartbeat.sh cccs18001001-hb1

set -e

BASE="${1:-cccs18001001-hb1}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

S1="${BASE}-s1"   # success
S2="${BASE}-s2"   # long (SLA breach)
S3="${BASE}-s3"   # crash / timeout

echo "========================================================"
echo "  Heartbeat Demo — 3 Scenarios"
echo "  base cmpgn_cd : ${BASE}"
echo "========================================================"

# ── S1: Success ──────────────────────────────────────────────
echo ""
echo "▶  S1 · SUCCESS  [${S1}]"
echo "   interval=20s  duration=60s  → expect: job.complete ✅"
echo ""
uv run python "${SCRIPT_DIR}/heartbeat_demo.py" "${S1}" \
    --interval 20 --duration 60

# ── S2: Long (SLA breach) ─────────────────────────────────────
echo ""
echo "▶  S2 · LONG  [${S2}]"
echo "   interval=20s  duration=150s  → expect: complete but SLA breach ⚠️"
echo ""
uv run python "${SCRIPT_DIR}/heartbeat_demo.py" "${S2}" \
    --interval 20 --duration 150

# ── S3: Crash / Timeout ───────────────────────────────────────
echo ""
echo "▶  S3 · CRASH  [${S3}]"
echo "   interval=20s  kill after 50s  → expect: heartbeat stops, no complete 💀"
echo ""
uv run python "${SCRIPT_DIR}/heartbeat_demo.py" "${S3}" \
    --interval 20 --duration 300 &
S3_PID=$!
echo "   [PID ${S3_PID}] running... will kill in 50s"
sleep 50
kill "${S3_PID}" 2>/dev/null || true
wait "${S3_PID}" 2>/dev/null || true
echo "   💀 Process killed — no job.complete sent"

# ── Done ─────────────────────────────────────────────────────
echo ""
echo "========================================================"
echo "  All 3 scenarios sent to App Insights."
echo "  Wait 2-5 min then check with kql-heartbeat.md — Query D"
echo ""
echo "  cmpgn_cd to search: ${BASE}"
echo "========================================================"
