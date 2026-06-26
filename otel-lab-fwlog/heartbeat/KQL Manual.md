# KQL Manual — Heartbeat Monitoring Queries

## ข้อมูลดิบที่มีใน App Insights

spans ทั้งหมดไปลงที่ตาราง `dependencies` (SpanKind.INTERNAL):

```
timestamp        | name           | customDimensions
─────────────────┼────────────────┼──────────────────────────────────
10:00:00         | job.heartbeat  | cmpgn_cd=...-s3, beat_number=1
10:00:20         | job.heartbeat  | cmpgn_cd=...-s3, beat_number=2
10:01:00         | job.complete   | cmpgn_cd=...-s1, elapsed_sec=60
10:02:30         | job.complete   | cmpgn_cd=...-s2, elapsed_sec=150
(ไม่มีแถวของ -s3 อีกเลย)
```

---

## Query B — หา job ที่ crash (ไม่มี complete)

**หลักคิด:** ถ้า job ยังมีชีวิตอยู่ → ต้องมี heartbeat มาเรื่อยๆ  
job ที่ crash = **มี heartbeat แต่ไม่มี complete/failed** + **heartbeat ล่าสุดนานเกินไปแล้ว**

```
dependencies (heartbeat)        dependencies (complete/failed)
─────────────────────────       ──────────────────────────────
-s1  last_beat=10:01            -s1  finished ✓
-s2  last_beat=10:03            -s2  finished ✓
-s3  last_beat=10:00:20         (ไม่มี -s3)
        │
        └── LEFT ANTI JOIN  →  เหลือแค่ -s3
                                (คือแถวที่ไม่มีคู่ใน finished)
                                then WHERE minutes_since_last_beat > threshold
```

**เครื่องมือหลัก:**

- `summarize max(timestamp) by cmpgn_cd` → หา heartbeat ล่าสุดต่อ campaign
- `join kind=leftanti (...)` → กรองเฉพาะที่ยังไม่จบ
- `datetime_diff("minute", now(), last_heartbeat) > threshold` → ห่างเกินไป = crash

**คำนวณ threshold:**

```
stale_threshold_min = --interval (วินาที) × 3 ÷ 60

demo (--interval 20s) : 20 × 3 ÷ 60 = 1 min
prod (--interval 30s) : 30 × 3 ÷ 60 = 1.5 min
prod (--interval 120s): 120 × 3 ÷ 60 = 6 min
```

---

## Query C — หา job ที่เสร็จแต่เกิน SLA

**หลักคิด:** ดูแค่ `job.complete` + เช็ค `elapsed_sec` ที่ script ฝังมาใน span

```
dependencies (job.complete เท่านั้น)
─────────────────────────────────────
-s1  elapsed_sec=60   →  60/60 = 1.0 min  ≤ SLA  ✅
-s2  elapsed_sec=150  →  150/60 = 2.5 min > SLA  ⚠️
                              ↑
                    WHERE elapsed_min > sla_min
```

**เครื่องมือหลัก:**

- filter `name == "job.complete"` — ดูแค่ที่จบสำเร็จ
- `toreal(customDimensions["elapsed_sec"]) / 60.0` — แปลงเป็นนาที
- `WHERE elapsed_min > sla_min` — เกิน threshold = SLA breach

---

## Query D — Summary ทุก campaign ใน run เดียว

**หลักคิด:** รวม B และ C เข้าด้วยกัน แล้วใส่ `case()` ตัดสินสถานะ

```
heartbeat summary          completion summary
(beats + last_beat)        (outcome + elapsed_min)
        │                          │
        └──── LEFT OUTER JOIN ─────┘
               on cmpgn_cd
                    │
              case() ตัดสิน:
              ┌─────────────────────────────────────────────┐
              │ complete + elapsed ≤ SLA  → ✅ Success      │
              │ complete + elapsed > SLA  → ⚠️ SLA breach   │
              │ failed                    → ❌ Failed        │
              │ ไม่มี complete + beat เก่า → 💀 Crashed      │
              │ ไม่มี complete + beat ใหม่ → 🔄 Still running│
              └─────────────────────────────────────────────┘
```

**ทำไมใช้ `LEFT OUTER` (ไม่ใช่ `LEFT ANTI`):**

- Query B ต้องการ "แถวที่ไม่มีคู่" → `leftanti`
- Query D ต้องการ "ทุกแถว พร้อม completion info ถ้ามี" → `leftouter` (null ถ้ายังไม่จบ)



---

## Diagnostic Query (ใช้เช็คก่อนรัน B/C/D)

```kusto
// ดูว่ามี records จริงไหม และ field ชื่ออะไร
dependencies
| where timestamp > ago(24h)
| where name in ("job.heartbeat","job.complete","job.failed")
| project timestamp, name, operation_Name,
          cmpgn_cd = tostring(customDimensions["cmpgn_cd"])
| order by timestamp desc
| take 20
```

