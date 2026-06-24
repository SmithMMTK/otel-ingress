# Heartbeat Demo — Campaign Job Monitoring

## ภาพรวม

demo นี้แสดง concept การ monitor **long-running campaign jobs** โดยส่ง heartbeat span ระหว่างที่ job กำลังทำงาน — แทนที่จะรอให้ job เสร็จก่อนถึงจะรู้ว่ามีปัญหา

```
timeline:
  0s       20s      40s      60s             150s
  │        │        │        │               │
  ▼        ▼        ▼        ▼               ▼
job.start  💓hb#1  💓hb#2  💓hb#3  ...  job.complete
                                   ↑
                           ถ้าหยุดตรงนี้ → 💀 crash detected
```

---

## โครงสร้างไฟล์

```
heartbeat/
├── heartbeat_demo.py   ← script หลัก (รัน 1 campaign)
├── scenarios.py        ← รัน 3 scenarios พร้อมกัน
├── kql-heartbeat.md    ← KQL queries สำหรับ detect issues
└── README.md           ← ไฟล์นี้
```

---

## 3 Scenarios

| Scenario | cmpgn_cd suffix | พฤติกรรม | ผลลัพธ์ที่คาดหวัง |
|----------|----------------|----------|-----------------|
| **S1 Success** | `-s1` | รัน 60s, beat ทุก 20s | ✅ Complete ปกติ |
| **S2 Long** | `-s2` | รัน 150s (เกิน SLA 90s), beat ทุก 20s | ⚠️ Complete แต่ SLA breach |
| **S3 Crash** | `-s3` | รัน 50s แล้ว process ถูก kill | 💀 Heartbeat หยุด ไม่มี complete |

---

## วิธีรัน

```bash
cd otel-lab-fwlog/heartbeat

# รัน 3 scenarios ทีเดียว (ใช้เวลา ~5 นาที)
python scenarios.py cccs18001001-hb1

# หรือรัน manual ทีละ scenario
python heartbeat_demo.py cccs18001001-hb1-s1 --interval 20 --duration 60
python heartbeat_demo.py cccs18001001-hb1-s2 --interval 20 --duration 150
python heartbeat_demo.py cccs18001001-hb1-s3 --interval 20 --duration 300
# (แล้ว Ctrl+C หลัง 50s เพื่อ simulate crash)
```

---

## ดูผลใน App Insights

หลังรันเสร็จรอ 2-5 นาที แล้วเปิด App Insights → Logs ใช้ queries จาก `kql-heartbeat.md`:

| Query | ดูอะไร |
|-------|--------|
| **A** | Heartbeat events ทั้งหมดต่อ campaign |
| **B** | 🚨 Detect crashed jobs (heartbeat หยุด + ไม่มี complete) |
| **C** | ⚠️ Jobs ที่ complete แต่เกิน SLA |
| **D** | Summary S1/S2/S3 side-by-side |

---

## Shared operationId กับ fwlog pipeline

`heartbeat_demo.py` อ่าน `operationMap.csv` จาก parent folder — ถ้า `cmpgn_cd` เดียวกับที่รันใน fwlog จะ **share trace_id เดียวกัน** ทำให้เห็น heartbeat + pipeline steps ใน E2E view เดียวกัน

---

## Parameters ของ heartbeat_demo.py

| Parameter | Default | คำอธิบาย |
|-----------|---------|----------|
| `cmpgn_cd` | (required) | รหัส campaign |
| `--interval` | 30 | ส่ง heartbeat ทุกกี่วินาที |
| `--duration` | 120 | job ทำงานนานเท่าไหร่ (วินาที) |
| `--fail` | false | simulate job failure ตอนจบ |
