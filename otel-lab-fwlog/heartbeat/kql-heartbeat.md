# KQL Queries — Heartbeat Monitoring

> **Concept**: ส่ง span ระหว่างที่ job กำลังรัน เพื่อ detect ว่า job ค้างหรือเปล่า
>
> | Span name | Kind | เมื่อไหร่ |
> |-----------|------|----------|
> | `<cmpgn_cd>` | SERVER | ตอน job เริ่ม (`job.start`) |
> | `job.heartbeat` | INTERNAL | ทุก N วินาที ขณะ job กำลังรัน |
> | `job.complete` | INTERNAL | เมื่อ job สำเร็จ |
> | `job.failed` | INTERNAL | เมื่อ job failed |

---

## Query A — ดู Heartbeat Events ทั้งหมด per Campaign
```kusto
// เปลี่ยน prefix เป็น cmpgn_cd ที่ต้องการ
let prefix = "cccs18001001-hb1";
union requests, dependencies
| where timestamp > ago(1h)
| where operation_Name startswith prefix
| extend cmpgn_cd   = tostring(customDimensions["cmpgn_cd"])
| extend beat_no    = tostring(customDimensions["beat_number"])
| extend elapsed    = tostring(customDimensions["elapsed_sec"])
| project timestamp, cmpgn_cd, event = name,
          beat_no, elapsed, itemType, operation_Id
| order by cmpgn_cd asc, timestamp asc
```

---

## Query B — 🚨 Detect Crashed / Timed-out Jobs (no heartbeat + no completion)
```kusto
// stale_threshold = interval * 3  (demo ใช้ --interval 20s → 60s → 1 min)
let stale_threshold_min = 1;
dependencies
| where timestamp > ago(24h)
| where name == "job.heartbeat"
| extend cmpgn_cd = tostring(customDimensions["cmpgn_cd"])
| summarize last_heartbeat = max(timestamp), total_beats = count() by cmpgn_cd
| join kind=leftanti (
    dependencies
    | where timestamp > ago(24h)
    | where name in ("job.complete", "job.failed")
    | extend cmpgn_cd = tostring(customDimensions["cmpgn_cd"])
    | summarize by cmpgn_cd
) on cmpgn_cd
| extend minutes_since_last_beat = datetime_diff("minute", now(), last_heartbeat)
| where minutes_since_last_beat > stale_threshold_min
| project cmpgn_cd, last_heartbeat,
          minutes_since_last_beat, total_beats,
          status = "💀 Crashed / Timed-out"
| order by minutes_since_last_beat desc
```

> **Alert config**: Aggregated logs, Table rows **> 0**, evaluate every 5 min

---

## Query C — ⚠️ Detect Long-running Jobs (completed but exceeded SLA)
```kusto
let sla_min = 1.5;  // ปรับตาม SLA จริง (นาที)
dependencies
| where timestamp > ago(24h)
| where name == "job.complete"
| extend cmpgn_cd   = tostring(customDimensions["cmpgn_cd"])
| extend elapsed_min = toreal(customDimensions["elapsed_sec"]) / 60.0
| where elapsed_min > sla_min
| project timestamp, cmpgn_cd, elapsed_min = round(elapsed_min, 1),
          total_beats = toint(customDimensions["total_beats"])
| order by elapsed_min desc
```

---

## Query D — Summary: S1 vs S2 vs S3 Side-by-side
```kusto
// ⚠️ เปลี่ยน "cccs18001001-hb1" ให้ตรงกับ base cmpgn_cd ที่ใช้
dependencies
| where timestamp > ago(24h)
| where name == "job.heartbeat"
| where tostring(customDimensions["cmpgn_cd"]) startswith "cccs18001001-hb1"
| extend cmpgn_cd = tostring(customDimensions["cmpgn_cd"])
| summarize beats = count(), last_beat = max(timestamp) by cmpgn_cd
| join kind=leftouter (
    dependencies
    | where timestamp > ago(24h)
    | where name in ("job.complete","job.failed")
    | where tostring(customDimensions["cmpgn_cd"]) startswith "cccs18001001-hb1"
    | extend cmpgn_cd = tostring(customDimensions["cmpgn_cd"])
    | project cmpgn_cd, outcome = name,
              elapsed_min = round(toreal(customDimensions["elapsed_sec"]) / 60.0, 1)
) on cmpgn_cd
| extend minutes_since_beat = round(datetime_diff("second", now(), last_beat) / 60.0, 1)
| extend status = case(
    outcome == "job.complete" and elapsed_min <= 1.5, "✅ Success (within SLA)",
    outcome == "job.complete" and elapsed_min >  1.5, "⚠️  Completed but SLA exceeded",
    outcome == "job.failed",                          "❌ Failed",
    minutes_since_beat > 2,                           "💀 Crashed (heartbeat stopped)",
    "🔄 Still running")
| project cmpgn_cd, status, beats, elapsed_min, last_beat
| order by cmpgn_cd asc
```
