# campaign-view — OpenTelemetry Campaign Trace Demo

สคริปต์นี้ส่ง trace ของ pipeline job (ingestion → transformation → kafka)
ไปยัง **Azure Application Insights** โดยใช้ OpenTelemetry

ออกแบบมาเพื่อ **ศึกษา** วิธีการ map business data (CSV) เข้า OTel trace model
และสามารถ **ต่อยอด** ไปใช้กับ pipeline จริงได้

---

## โครงสร้างไฟล์

```
campaign-view/
├── fwlog_campaign.py     ← สคริปต์หลัก
├── operationMap.csv      ← mapping: cmpgn_cd → operationId (auto-managed)
├── fwlog-001.csv         ← ingestion + api_create records
├── fwlog-002.csv         ← transformation records
├── fwlog-003.csv         ← kafka records
└── README.md
```

---

## วิธีรัน

```bash
# รัน CSV ทีละไฟล์ ระบุ CAMPAIGN_NAME เป็น arg ที่ 2
python fwlog_campaign.py fwlog-001.csv Campaign-g8
python fwlog_campaign.py fwlog-002.csv Campaign-g8
python fwlog_campaign.py fwlog-003.csv Campaign-g8
```

> **CAMPAIGN_NAME** คือชื่อที่จะปรากฏใน App Insights ช่อง **Operation Name**
> ถ้าไม่ระบุ default = `"Campaign"`

---

## โครงสร้าง CSV

```csv
process,pos_dt,load_tms,cmpgn_cd,count_lead,duration
ingestion,2026-06-24,2026-06-24T08:00:00+07:00,cccs18001001-g8,100,10
```

| Column | ความหมาย | ใช้ใน OTel |
|---|---|---|
| `process` | ชื่อ step | span name (child) |
| `pos_dt` | วันที่ position | custom attribute |
| `load_tms` | เวลาเริ่มต้น step | `start_time` ของ span |
| `cmpgn_cd` | campaign code + version | operationMap key, custom attribute |
| `count_lead` | จำนวน lead | custom attribute |
| `duration` | ระยะเวลา (นาที) | คำนวณ `end_time = start_time + duration` |

---

## การทำงานภายใน (Step by Step)

### 1. โหลด .env
สคริปต์ค้นหา `.env` จาก directory ปัจจุบันขึ้นไป 3 ระดับ
ต้องมี `APPLICATIONINSIGHTS_CONNECTION_STRING`

```
campaign-view/.env  ← ถ้ามี
otel-lab-fwlog/.env
otel-databricks/.env  ← ถ้าอยู่ใน monorepo
```

### 2. อ่าน CSV และ resolve operationId

```
cmpgn_cd = "cccs18001001-g8"
    ↓
ค้นหาใน operationMap.csv
    ├── พบ → ใช้ operationId เดิม (trace ต่อเนื่อง across CSV files)
    └── ไม่พบ → สร้าง uuid4().hex ใหม่ → บันทึกลง operationMap.csv
```

**หลักการสำคัญ:** `operationId` เดียวกันใน fwlog-001, 002, 003
= spans ทั้งหมดอยู่ใน trace เดียวกัน (E2E transaction view)

### 3. จัดกลุ่ม records ตาม cmpgn_cd

```python
groups = {
    "cccs18001001-g8": [row_ingestion, row_api_create, ...],
    "cccs18001002-g8": [row_ingestion, ...],
}
```

### 4. สร้าง Span Hierarchy

```
SpanContext (remote, trace_id จาก operationId)   ← synthetic parent
    │
    └── Campaign-g8  [SpanKind.SERVER]            ← root span
            │                                        → operation_Name = "Campaign-g8"
            ├── ingestion   [SpanKind.INTERNAL]   ← child span
            ├── api_create  [SpanKind.INTERNAL]      start_time = load_tms
            └── transformation [SpanKind.INTERNAL]   end_time = load_tms + duration
```

**SpanKind.SERVER** → App Insights จัดเก็บใน `requests` table
→ ค่า `operation_Name` ถูกกำหนดจาก span นี้

**SpanKind.INTERNAL** → App Insights จัดเก็บใน `dependencies` table
→ inherit `operation_Name` และ `operation_Id` จาก root

### 5. Timing

```python
start_time_ns = datetime.fromisoformat(load_tms).timestamp() * 1e9
end_time_ns   = start_time_ns + (duration_min * 60 * 1e9)
```

ใช้ **historical timestamp** จาก CSV (ไม่ใช่เวลาปัจจุบัน)
→ spans จะปรากฏย้อนหลังใน App Insights (ใช้ "Last 7 days" เพื่อดู)

### 6. Flush & Shutdown

```python
tracer_provider.force_flush(timeout_millis=15000)
tracer_provider.shutdown()
```

**จำเป็นมาก** — ถ้าไม่ flush spans ที่ยังค้างใน queue จะหาย

---

## OTel → App Insights Mapping

| OTel | App Insights | ดูได้ที่ |
|---|---|---|
| Root span name (SERVER) | `operation_Name` | Operations blade |
| `trace_id` (hex 32 chars) | `operation_Id` | E2E Transaction |
| Child span name (INTERNAL) | `name` ใน `dependencies` | Transaction detail |
| `start_time` / `end_time` | `timestamp` / `duration` | Timeline |
| Span attributes | `customDimensions` | Properties panel |

---

## operationMap.csv — หัวใจของ Cross-file Correlation

```csv
cmpgn_cd,operationId
cccs18001001-g8,c2d8514fae40462b9508a7169ed4247a
cccs18001002-g8,2cfb2f3e8b41492781889cf85b6fe425
```

- **Key** = versioned `cmpgn_cd` → แต่ละรอบ run ใช้ trace แยกกัน
- รัน fwlog-001 ก่อน → สร้าง operationId
- รัน fwlog-002, 003 ทีหลัง → ใช้ operationId เดิม → spans ต่อใน trace เดิม
- ไฟล์นี้ **persistent** ระหว่าง script runs (ไม่ลบระหว่างรัน)

---

## Azure Monitor Batching

สคริปต์ไม่ส่ง span ทีละ record แต่ใช้ **BatchSpanProcessor**:

```
spans → in-memory queue (default max 2,048)
              ↓
     force_flush() / batch เต็ม (512) / timeout (5s)
              ↓
     HTTP POST → Application Insights
     (1 POST มีสูงสุด 512 spans)
```

> ⚠️ ถ้า CSV มีมากกว่า 2,000 records ต้องปรับ `max_queue_size`:
> ```python
> # ใน configure_azure_monitor() หรือสร้าง BatchSpanProcessor เอง
> max_queue_size=10000
> ```

---

## ต่อยอดได้อย่างไร

### เพิ่ม step ใหม่
เพิ่ม row ใน CSV ด้วย `process` ชื่อใหม่ เช่น `"validation"` — ไม่ต้องแก้ code

### เพิ่ม attribute ใหม่
เพิ่ม column ใน CSV — สคริปต์อ่าน **ทุก column** เป็น custom attribute อัตโนมัติ

### เปลี่ยน Operation Name แบบ dynamic
```bash
python fwlog_campaign.py fwlog-001.csv "MyPipeline-2026Q3"
```

### ส่งจาก pipeline จริง (ไม่ใช่ CSV)
แทนที่ `csv.DictReader` ด้วย DataFrame หรือ query result:
```python
records = spark_df.collect()  # Databricks
# หรือ
records = db_cursor.fetchall()  # Database
```

### เพิ่ม error span
```python
child.set_status(StatusCode.ERROR, "error message")
child.record_exception(exception)
```

---

## KQL ดู result ใน App Insights

```kql
-- ดู root spans (operation_Name)
requests
| where timestamp > ago(24h)
| where operation_Name startswith "Campaign"
| project timestamp, operation_Name, operation_Id, duration

-- ดู child spans พร้อม custom properties
dependencies
| where timestamp > ago(24h)
| where operation_Name startswith "Campaign"
| extend cmpgn_cd = tostring(customDimensions["cmpgn_cd"])
| project timestamp, name, cmpgn_cd, duration, operation_Id
| order by timestamp desc
```
