# KQL Queries — fwlog App Insights Monitoring

> **Tables used**
>
> - `dependencies` — child spans (process steps: ingestion, api_create, transformation, kafka)
> - `requests`     — root spans (one per cmpgn_cd per CSV file run)
>
> **Key fields in `dependencies`**
>
> | Field                              | Description                                                    |
> | ---------------------------------- | -------------------------------------------------------------- |
> | `name`                             | process step (ingestion / api_create / transformation / kafka) |
> | `operation_Name`                   | cmpgn_cd                                                       |
> | `operation_Id`                     | trace id — shared across all CSV files for same cmpgn_cd       |
> | `duration`                         | real, milliseconds (end − start)                               |
> | `customDimensions["duration_min"]` | raw duration from CSV (minutes, string)                        |
> | `customDimensions["count_lead"]`   | lead count from CSV                                            |
> | `customDimensions["pos_dt"]`       | process date from CSV                                          |
> | `customDimensions["cmpgn_cd"]`     | campaign code                                                  |
> | `customDimensions["source_file"]`  | CSV filename that produced this span                           |

---

## 1. Overall Performance — Dependency Stats

### 1.1 Percentile + Run Count per Process Step (ดูภาพรวม performance)

```kusto
// SLA thresholds (minutes) — ปรับตาม SLA จริง
let sla = datatable(step:string, warn_min:real, crit_min:real)
[
    "ingestion",       20, 30,
    "api_create",      45, 60,
    "transformation",  30, 45,
    "kafka",           60, 90
];
dependencies
| where timestamp > ago(7d)
| where name in ("ingestion", "transformation","kafka")
| extend dur_min = duration / 60000.0
| summarize
    runs     = count(),
    p50_min  = round(percentile(dur_min, 50), 1),
    p75_min  = round(percentile(dur_min, 75), 1),
    p95_min  = round(percentile(dur_min, 95), 1),
    max_min  = round(max(dur_min), 1),
    avg_min  = round(avg(dur_min), 1)
    by step = name
| join kind=leftouter sla on step
| extend recommendation = case(
    p95_min > crit_min, "🔴 Investigate — P95 exceeds critical SLA",
    p95_min > warn_min, "🟡 Monitor — P95 approaching SLA limit",
    "🟢 Normal")
| project step, runs, avg_min, p50_min, p75_min, p95_min, max_min,
          warn_min, crit_min, recommendation
| order by p95_min desc
```

### 1.2 Duration Trend per Step (รายวัน — ดู pattern เมื่อเวลาผ่านไป)

```kusto
dependencies
| where timestamp > ago(30d)
| where name in ("ingestion","transformation","kafka")
| extend pos_dt  = todatetime(customDimensions["pos_dt"])
| extend dur_min = duration / 60000.0
| summarize
    avg_min = round(avg(dur_min), 1),
    p95_min = round(percentile(dur_min, 95), 1),
    runs    = count()
    by pos_dt, step = name
| order by pos_dt desc, step asc
```

### 1.3 Slowest Runs (top 20 worst individual executions)

```kusto
dependencies
| where timestamp > ago(7d)
| where name in ("ingestion","transformation","kafka")
| extend dur_min    = round(duration / 60000.0, 1)
| extend cmpgn_cd   = tostring(customDimensions["cmpgn_cd"])
| extend pos_dt     = todatetime(customDimensions["pos_dt"])
| project timestamp, pos_dt, step = name, cmpgn_cd, dur_min, operation_Id
| top 20 by dur_min desc
```

---

## 2. Pipeline State (สถานะ Campaign)

### 2.1 Pipeline Duration per Campaign — Pivot (minutes summed per step)
```kusto
dependencies
| where name in ("ingestion","transformation","kafka")
| where timestamp > ago(24h)
| extend cmpgn_cd = tostring(customDimensions["cmpgn_cd"])
| extend dur_min  = duration / 60000.0
| summarize
    ingestion      = sumif(dur_min, name == "ingestion"),
    transformation = sumif(dur_min, name == "transformation"),
    kafka          = sumif(dur_min, name == "kafka"),
    first_seen     = min(timestamp)
    by cmpgn_cd
| project cmpgn_cd,
          ingestion      = round(ingestion, 1),
          transformation = round(transformation, 1),
          kafka          = round(kafka, 1),
          timestamp      = first_seen
| order by timestamp desc
```

### 2.2 count_lead per Step per Campaign — Pivot
```kusto
dependencies
| where name in ("ingestion","transformation","kafka")
| where timestamp > ago(24h)
| extend cmpgn_cd   = tostring(customDimensions["cmpgn_cd"])
| extend count_lead = toint(customDimensions["count_lead"])
| summarize
    ingestion      = sumif(count_lead, name == "ingestion"),
    transformation = sumif(count_lead, name == "transformation"),
    kafka          = sumif(count_lead, name == "kafka"),
    total          = sum(count_lead),
    first_seen     = min(timestamp)
    by cmpgn_cd
| project cmpgn_cd, ingestion, transformation, kafka, total,
          timestamp = first_seen
| order by timestamp desc
```

### 2.3 count_lead per Process per cmpgn_cd (detail)

```kusto
dependencies
| where timestamp > ago(7d)
| where name in ("ingestion","transformation","kafka")
| extend cmpgn_cd   = tostring(customDimensions["cmpgn_cd"])
| extend count_lead = toint(customDimensions["count_lead"])
| extend pos_dt     = todatetime(customDimensions["pos_dt"])
| summarize total_lead = sum(count_lead) by cmpgn_cd, pos_dt, step = name
| order by cmpgn_cd asc, pos_dt asc, step asc
```

### 2.4 Daily Lead Volume Trend per Process (30 วันย้อนหลัง)

```kusto
dependencies
| where timestamp > ago(30d)
| where name in ("ingestion","transformation","kafka")
| extend pos_dt     = todatetime(customDimensions["pos_dt"])
| extend count_lead = toint(customDimensions["count_lead"])
| summarize daily_lead = sum(count_lead) by pos_dt, step = name
| order by pos_dt desc, step asc
```

---

## 3. Abnormal Operations (ใช้สำหรับ Alert)

### 3.1 Steps ที่ใช้เวลาเกิน SLA Threshold (Alert-ready)
> ⚠️ Azure Monitor Alert **ไม่รองรับ `let` / `datatable`** — ใช้ `case()` แทน
> Alert config: **Aggregated logs**, Measure = **Table rows**, Threshold **> 0**

```kusto
dependencies
| where timestamp > ago(24h)
| where name in ("ingestion", "transformation", "kafka")
| extend actual_min = duration / 60000.0
| extend max_min = case(
                       name == "ingestion",
                       8.0,
                       name == "transformation",
                       45.0,
                       name == "kafka",
                       90.0,
                       0.0
                   )
| where actual_min > max_min
| project
    TimeGenerated = timestamp,
    operation_Name,
    step = name,
    actual_min,
    max_min,
    cmpgn_cd    = tostring(customDimensions["cmpgn_cd"]),
    source_file = tostring(customDimensions["source_file"]),
    operation_Id
```

### 3.2 Campaign ที่ขาด Process Step (Pipeline ไม่ครบ)

```kusto
// group by cmpgn_cd (customDimensions) ไม่ใช่ operation_Name
// และ expected_steps ต้องไม่มี api_create
let expected_steps = dynamic(["ingestion","transformation","kafka"]);
dependencies
| where timestamp > ago(24h)
| where name in ("ingestion","transformation","kafka")
| extend cmpgn_cd = tostring(customDimensions["cmpgn_cd"])
| summarize completed_steps = make_set(name) by cmpgn_cd
| extend missing_steps = set_difference(expected_steps, completed_steps)
| where array_length(missing_steps) > 0
| project cmpgn_cd, completed_steps, missing_steps
| order by cmpgn_cd asc
```

### 3.3 Duration สั้นผิดปกติ (< 1 นาที — อาจเป็น data issue)

```kusto
dependencies
| where timestamp > ago(24h)
| where name in ("ingestion","transformation","kafka")
| extend actual_min = duration / 60000.0
| where actual_min < 1
| project timestamp, operation_Name, name, actual_min,
          duration_min_raw = tostring(customDimensions["duration_min"]),
          operation_Id
| order by timestamp desc
```

### 3.4 Duplicate Process Steps (same cmpgn_cd run เกิน 1 ครั้ง)

```kusto
dependencies
| where timestamp > ago(24h)
| summarize count_runs = count() by operation_Name, name
| where name in ("ingestion","transformation", "kafka") and count_runs > 1
| project cmpgn_cd = operation_Name, step = name, count_runs
| order by count_runs desc
```

### 3.5 Failed Operations (success = false)

```kusto
dependencies
| where timestamp > ago(24h)
| where name in ("ingestion","transformation","kafka")
| where success == false
| project timestamp, operation_Name, name,
          cmpgn_cd    = tostring(customDimensions["cmpgn_cd"]),
          operation_Id, itemId
| order by timestamp desc
```

### 3.6 Stale Campaigns — ไม่มี activity เกิน N ชั่วโมง

```kusto
let stale_hours = 6;
dependencies
| where timestamp > ago(7d)
| where name in ("ingestion","transformation","kafka")
| summarize last_activity = max(timestamp) by cmpgn_cd = operation_Name
| extend hours_since = datetime_diff("hour", now(), last_activity)
| where hours_since > stale_hours
| project cmpgn_cd, last_activity, hours_since
| order by hours_since desc
```

---

## 4. Ad-hoc / Utilities

### 4.1 End-to-end Trace ของ cmpgn_cd ที่ต้องการ

```kusto
let target = "cccs18001001-f12";  // เปลี่ยนตาม cmpgn_cd ที่ต้องการ
union requests, dependencies
| where name in ("ingestion","transformation","kafka")
| where operation_Name == target
| project timestamp,
          type        = itemType,
          step        = name,
          actual_min  = round(duration / 60000.0, 2),
          count_lead  = tostring(customDimensions["count_lead"]),
          source_file = tostring(customDimensions["source_file"]),
          operation_Id
| order by timestamp asc
```

### 4.2 Recent Operations Overview (24 ชั่วโมงล่าสุด)

```kusto
requests
| where timestamp > ago(24h)
| project timestamp, cmpgn_cd = name,
          duration_min = round(duration / 60000.0, 1),
          success, operation_Id,
          source_file = tostring(customDimensions["source_file"])
| order by timestamp desc
```

### 4.3 Throughput — จำนวน Campaign ที่ Process ต่อชั่วโมง

```kusto
requests
| where timestamp > ago(7d)
| summarize campaigns = dcount(name) by bin(timestamp, 1h)
| order by timestamp desc
```

### 4.4 Sanity Check — CSV duration_min vs Actual Span Duration

```kusto
dependencies
| where timestamp > ago(24h)
| where name in ("ingestion","transformation","kafka")
| extend csv_dur_min    = toreal(customDimensions["duration_min"])
| extend actual_dur_min = round(duration / 60000.0, 2)
| extend diff_min       = round(actual_dur_min - csv_dur_min, 2)
| where abs(diff_min) > 0.1
| project timestamp, operation_Name, name,
          csv_dur_min, actual_dur_min, diff_min
| order by abs(diff_min) desc
```