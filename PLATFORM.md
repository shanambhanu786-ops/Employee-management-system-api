# Employee analytics platform (Spark + Iceberg + CRM)

```
MongoDB (live API, primary) --secondary reads--> Spark[etl] --MERGE--> Iceberg lake.hr.*
                                                                     |--> marts (dept_monthly)
Iceberg --> Spark[alerts] --> lake.ops.alerts --HTTP--> CRM (cases + SLA clocks, :8001) --> Slack webhook
Spark[interactive] <-- ad-hoc / dashboards on Iceberg        every job --> lake.ops.job_runs (SLA ledger)
```

## Run
```bash
pip install -r requirements-platform.txt          # also needs Java 11/17
export MONGO_URI=... MONGO_DB=attendance_db ICEBERG_WAREHOUSE=s3a://bucket/warehouse
python -m lakehouse.pipeline all --full           # first load; later: all (45-day lookback)
python -m lakehouse.alerts                        # every 5-15 min
python -m lakehouse.pipeline maintain             # nightly
uvicorn crm.app:app --port 8001                   # CRM; set CRM_API_KEY, CRM_URL, ALERT_WEBHOOK_URL
curl -X POST localhost:8001/crm/sla/escalate      # every 5 min (cron)
```

## Workload isolation (3 layers)
| Layer | Where | Effect |
|---|---|---|
| Separate apps per profile | `PROFILES` in `session.py`, `k8s/isolation.yaml` | own executor caps, own namespace/queue, ResourceQuota, PriorityClass |
| FAIR pools | `conf/fairscheduler.xml` | alerts (weight 5) > interactive (3) > etl (1) inside an app |
| Mongo secondaries | `readPreference=secondaryPreferred` | analytics never loads the primary serving the API |

## Iceberg query optimisation
| Feature | Used for |
|---|---|
| `months(att_date)` + `bucket(16, emp_code)` hidden partitioning | month scans prune partitions; single-employee lookups touch 1 bucket; queries never mention partitions |
| `WRITE ORDERED BY emp_code, att_date` + `rewrite_data_files(strategy=>'sort')` | tight min/max stats, so files are skipped |
| merge-on-read + changed-rows-only MERGE | cheap daily upserts; compaction folds delete files |
| `rewrite_manifests`, `expire_snapshots`, `remove_orphan_files` | small metadata, bounded storage (7-day time travel, last 20 snapshots) |
| `overwritePartitions()` on `dept_monthly` | idempotent rebuild of only the touched months |

Inspect: `SELECT * FROM lake.hr.attendance.files`, `.partitions`, `.snapshots`.

## Alerts -> CRM
`EXCESSIVE_LATE`, `CONSECUTIVE_ABSENT`, `MISSING_PUNCH_OUT`, `JOB_SLA_BREACH`, `PIPELINE_STALE` (thresholds in `alerts.py`).
Severity maps to priority: CRITICAL->P1, HIGH->P2, WARNING->P3. Alerts are stored, then pushed; failed pushes retry
(`crm_synced=false`), and the CRM de-duplicates on `source_alert_key`.

## Boundary SLAs
| Boundary | SLA | Enforced by |
|---|---|---|
| Mongo -> lake (ingest) | data <= 1h old, run <= 15 min | `PIPELINE_STALE`, `JOB_SLA_BREACH` |
| Lake -> marts | data <= 24h old, run <= 20 min | same |
| Alerts job | run <= 5 min | `JOB_SLA_BREACH` (the job itself needs an external heartbeat) |
| Lake -> CRM case | P1 respond 1h / resolve 4h, P2 4h / 24h (24x7); P3 8h / 3d, P4 24h / 5d (business hours) | `/crm/sla/*` |

Case clocks show `PENDING / AT_RISK (<25% left) / BREACHED / MET`; `POST /crm/sla/escalate` escalates each breach once.

## Assumptions
- "Trading employee" read as employee analytics for this attendance data (no market data involved).
- CRM = HR case management (cases, owners, SLAs), not sales CRM.
- No holiday calendar in business-hours SLAs.
- Not run against a live Spark/Iceberg/Mongo stack; pin versions and smoke-test on a small dataset first.
