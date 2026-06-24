# fwlog — ส่ง Firewall Log ไปยัง Azure Application Insights

## ภาพรวม

`fwlog.py` อ่านไฟล์ CSV ที่มีข้อมูล campaign log แล้วส่งเป็น **Distributed Trace**
ไปยัง Azure Application Insights เพื่อดู End-to-End Transaction ต่อ `cmpgn_cd`

```
Application Insights — End-to-end transaction view

cccs18001001-f8  [══════════════════════════════════════]  ← root span (cmpgn_cd)
  ingestion      [██]                                       ← child span (process)
  api_create          [████]
  api_create                [████]
  transformation                  [██████]
  kafka                                   [████]
```

---

## สิ่งที่ต้องมีก่อนเริ่มต้น

| รายการ | รายละเอียด |
|--------|-----------|
| Python 3.11+ | ติดตั้งได้จาก https://www.python.org |
| [uv](https://docs.astral.sh/uv/) | Python package manager (`pip install uv` หรือ `curl -LsSf https://astral.sh/uv/install.sh \| sh`) |
| Azure Application Insights | ต้องมี resource พร้อม Connection String |

---

## โครงสร้างไฟล์

```
otel-lab-fwlog/
├── fwlog.py              ← script หลัก
├── operationMap.csv      ← ไฟล์ mapping cmpgn_cd → operationId (สร้างอัตโนมัติ)
├── fwlog-001.csv         ← ตัวอย่าง input file (step 1: ingestion, api_create)
├── fwlog-002.csv         ← ตัวอย่าง input file (step 2: transformation)
├── fwlog-003.csv         ← ตัวอย่าง input file (step 3: kafka)
└── .env                  ← config file (ต้องสร้างเอง — ห้าม commit)
```

---

## ขั้นตอนการติดตั้ง

### 1. Clone และเข้าโฟลเดอร์

```bash
git clone <repo-url>
cd otel-databricks/otel-lab-fwlog
```

### 2. ติดตั้ง dependencies

```bash
uv sync
```

> **หมายเหตุ:** ถ้ายังไม่มีไฟล์ `pyproject.toml` ให้รัน `uv init` ก่อน แล้วติดตั้ง:
> ```bash
> uv add azure-monitor-opentelemetry opentelemetry-sdk python-dotenv
> ```

### 3. สร้างไฟล์ `.env`

สร้างไฟล์ `.env` ในโฟลเดอร์ `otel-lab-fwlog/` (หรือที่ root ของ repo):

```env
APPLICATIONINSIGHTS_CONNECTION_STRING=InstrumentationKey=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx;IngestionEndpoint=https://...
```

> **หาค่า Connection String ได้จาก:**
> Azure Portal → Application Insights resource → Overview → Connection String (คลิก Copy)

---

## Schema ของไฟล์ CSV

| คอลัมน์ | ชนิด | ตัวอย่าง | คำอธิบาย |
|---------|------|---------|----------|
| `process` | string | `ingestion` | ชื่อ step/กระบวนการ → ใช้เป็นชื่อ child span |
| `pos_dt` | date | `2026-06-23` | วันที่ process |
| `load_tms` | ISO 8601 | `2026-06-23T12:20:00.459+07:00` | เวลาเริ่มต้นของ step นี้ (ใช้เป็น span start time) |
| `cmpgn_cd` | string | `cccs18001001-f8` | รหัส campaign → ใช้เป็น operation group |
| `count_lead` | int | `2` | จำนวน lead (เก็บเป็น custom property) |
| `duration` | int | `10` | ระยะเวลา **หน่วยนาที** → ใช้คำนวณ span end time |

### ตัวอย่าง CSV

```csv
process,pos_dt,load_tms,cmpgn_cd,count_lead,duration
ingestion,2026-06-23,2026-06-23T12:20:00.459+07:00,cccs18001001-f8,2,10
api_create,2026-06-23,2026-06-23T12:30:00.459+07:00,cccs18001001-f8,1,20
```

---

## วิธีรัน

```bash
# รันไฟล์เดียว
python fwlog.py fwlog-001.csv

# รันหลายไฟล์ต่อเนื่อง (แนะนำ — ได้ trace เต็ม pipeline)
python fwlog.py fwlog-001.csv
python fwlog.py fwlog-002.csv
python fwlog.py fwlog-003.csv
```

> script จะ auto-relaunch ตัวเองด้วย `uv run python` เพื่อให้แน่ใจว่าใช้ venv ที่ถูกต้อง
> และ flush data ได้ครบก่อน process จบ

### ตัวอย่าง output เมื่อรันสำเร็จ

```
Python: /Users/you/.venv/bin/python

  [cccs18001001-f8]  operationId=8928d8e3722a4e88baf6cf358b1c56e9
    → ingestion        2026-06-23T12:20:00.459+07:00  dur=10min
    → api_create       2026-06-23T12:30:00.459+07:00  dur=20min
    → api_create       2026-06-23T12:40:00.459+07:00  dur=20min

Transmission succeeded: Item received: 11. Items accepted: 11

Done: 7 span(s) from 'fwlog-001.csv' — 2 operation(s)
```

---

## ดูผลลัพธ์ใน Application Insights

1. เปิด **Azure Portal** → ไปที่ Application Insights resource
2. เลือก **Transaction search** หรือ **Investigate → Transaction search**
3. ตั้ง time range เป็น **"Last 2 hours"** หรือ **"Last 7 days"**
   (ค่า default "Last 30 minutes" อาจไม่พบถ้า timestamp ใน CSV เก่ากว่า 30 นาที)
4. ค้นหาด้วย `cmpgn_cd` เช่น `cccs18001001-f8`
5. คลิก transaction แล้วดู **End-to-end transaction** view

---

## operationMap.csv — การ share Operation ID ข้ามไฟล์

ไฟล์นี้เป็น **หัวใจของการ group trace** ข้ามหลาย CSV file

```
cmpgn_cd,operationId
cccs18001001-f8,8928d8e3722a4e88baf6cf358b1c56e9
cccs18001002-f8,fdd178697a2b4a20bc633c55f72aef9a
```

**กฎการทำงาน:**
- ถ้า `cmpgn_cd` **มีอยู่แล้ว** ใน `operationMap.csv` → ใช้ `operationId` เดิม
- ถ้า `cmpgn_cd` **ยังไม่มี** → สร้าง UUID ใหม่ แล้วบันทึกลงไฟล์

ผลลัพธ์คือ `fwlog-001.csv`, `fwlog-002.csv`, `fwlog-003.csv` ที่มี `cmpgn_cd` เดียวกัน
จะ **share trace_id เดียวกัน** และปรากฏเป็น transaction เดียวใน App Insights

> **สำคัญ:** เก็บไฟล์ `operationMap.csv` ไว้ร่วมกับ script เสมอ
> ถ้าลบไฟล์นี้ทิ้ง จะสูญเสียการ correlate ข้ามไฟล์

---

## การทำงานภายใน (สำหรับผู้ที่ต้องการปรับแต่ง)

```
CSV rows
   │
   ├─ group by cmpgn_cd
   │
   ├─ resolve operationId (จาก operationMap.csv)
   │
   ├─ derive trace_id = int(operationId[:32], 16)   ← 128-bit, fixed per cmpgn_cd
   │
   ├─ root span  (SpanKind.SERVER)
   │     name      = cmpgn_cd
   │     start     = min(load_tms ทุก row)
   │     end       = max(load_tms + duration)
   │     trace_id  = derived จาก operationId
   │
   └─ child spans (SpanKind.INTERNAL) per row
         name      = process
         start     = load_tms
         end       = load_tms + duration (นาที)
         attributes = ทุก field ใน CSV row
```

**App Insights field mapping:**

| OTel | App Insights |
|------|-------------|
| Root span name (SERVER) | `operation_Name` |
| `trace_id` | `operation_Id` |
| Child span name (INTERNAL) | dependency name |
| `load_tms` | span start timestamp |
| `duration` (นาที) | span duration |
| ทุก CSV field | custom dimensions |

---

## ข้อควรระวัง

| ประเด็น | รายละเอียด |
|---------|-----------|
| **Timestamp เก่าเกิน 48 ชั่วโมง** | App Insights ปฏิเสธ data ที่เก่ากว่า 48 ชั่วโมง |
| **Cold ingestion path** | Timestamp ที่เก่ากว่า 5 นาที จะใช้ cold path (ใช้เวลา 5-45 นาทีกว่าจะแสดง) |
| **ไม่พบ data ใน portal** | ตรวจสอบ time range — เปลี่ยนจาก "Last 30 min" เป็น "Last 7 days" |
| **`.env` ใน git** | ห้าม commit ไฟล์ `.env` เพราะมี Connection String (ใช้ `.gitignore`) |
| **operationMap.csv** | ควร commit ไว้ใน git เพื่อให้ทีมใช้ operationId เดียวกัน |

---

## Troubleshooting

**ปัญหา: `APPLICATIONINSIGHTS_CONNECTION_STRING not set`**
- ตรวจสอบว่ามีไฟล์ `.env` และมีบรรทัด `APPLICATIONINSIGHTS_CONNECTION_STRING=...`
- หรือ export ตัวแปรก่อนรัน: `export APPLICATIONINSIGHTS_CONNECTION_STRING="InstrumentationKey=..."`

**ปัญหา: `Transmission succeeded` แต่ไม่เห็น data ใน portal**
- ขยาย time range เป็น "Last 2 hours" หรือ "Last 7 days"
- รอ 2-5 นาที แล้ว refresh

**ปัญหา: แต่ละ CSV file ไม่ share trace เดียวกัน**
- ตรวจสอบว่า `operationMap.csv` มี entry ของ `cmpgn_cd` นั้นแล้ว
- ถ้า `cmpgn_cd` ใน CSV spelling ต่างกัน (เช่น trailing space) จะสร้าง operationId ใหม่

**ปัญหา: `uv: command not found`**
- ติดตั้ง uv: `pip install uv` หรือดูที่ https://docs.astral.sh/uv/getting-started/installation/
