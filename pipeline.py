"""
Mongo -> Iceberg pipeline.

  python -m lakehouse.pipeline ingest   [--lookback-days 45 | --full]   MERGE Mongo into Iceberg
  python -m lakehouse.pipeline marts    [--lookback-days 45 | --full]   rebuild department x month mart
  python -m lakehouse.pipeline maintain                                 compact, expire, clean up
  python -m lakehouse.pipeline all

Mongo has no updated_at, so ingest re-reads a lookback window (corrections can touch recent days)
and MERGEs only rows that actually changed. Re-running is idempotent.
Business rules match the API: stored derived values are read as-is; weekdays on/after joined_on only.
"""
import argparse
import json
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import functions as F

from .session import IST, JOB_SLA_SECONDS, MONGO_DB, build_session, ensure_tables, track_job

PRESENT = ["PRESENT", "WFH", "ON_DUTY"]
ATT_SCHEMA = ("emp_code string, date string, status string, punch_in timestamp, punch_out timestamp, "
              "work_hours double, late_minutes long, overtime_minutes long, half_day boolean")
EMP_SCHEMA = ("emp_code string, name string, email string, department string, "
              "shift_start string, shift_end string, joined_on string")


def today_ist() -> date:
    return datetime.now(IST).date()


def mongo(spark, coll, schema, pipeline=None):
    # a fresh reader every time: PySpark readers are mutable and would leak options
    r = (spark.read.format("mongodb").option("database", MONGO_DB)
         .option("collection", coll).schema(schema))
    if pipeline:
        r = r.option("aggregation.pipeline", json.dumps(pipeline))  # filter runs inside MongoDB
    return r.load()


def ingest(spark, lookback_days: int, full: bool):
    emp = mongo(spark, "employees", EMP_SCHEMA).select(
        "emp_code", "name", "email", "department", "shift_start", "shift_end",
        F.to_date("joined_on").alias("joined_on"))
    emp.writeTo("lake.hr.employees").using("iceberg").createOrReplace()

    cutoff = None if full else (today_ist() - timedelta(days=lookback_days)).isoformat()
    pipe = None if full else [{"$match": {"date": {"$gte": cutoff}}}]
    raw = mongo(spark, "attendance_logs", ATT_SCHEMA, pipe)
    src = (raw.select(
        "emp_code", F.to_date("date").alias("att_date"), "status", "punch_in", "punch_out", "work_hours",
        F.coalesce("late_minutes", F.lit(0)).cast("int").alias("late_minutes"),
        F.coalesce("overtime_minutes", F.lit(0)).cast("int").alias("overtime_minutes"),
        F.coalesce("half_day", F.lit(False)).alias("half_day"),
        F.current_timestamp().alias("updated_at"))
        .dropDuplicates(["emp_code", "att_date"]))
    src.createOrReplaceTempView("src_attendance")

    prune = "" if full else f"AND t.att_date >= DATE'{cutoff}'"  # lets Iceberg prune old partitions
    spark.sql(f"""
        MERGE INTO lake.hr.attendance t USING src_attendance s
        ON t.emp_code = s.emp_code AND t.att_date = s.att_date {prune}
        WHEN MATCHED AND s.updated_at >= t.updated_at AND (t.status <> s.status
              OR NOT (t.punch_in <=> s.punch_in) OR NOT (t.punch_out <=> s.punch_out)
              OR NOT (t.work_hours <=> s.work_hours) OR t.late_minutes <> s.late_minutes
              OR t.overtime_minutes <> s.overtime_minutes OR t.half_day <> s.half_day)
          THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")


def marts(spark, lookback_days: int, full: bool):
    today = today_ist()
    if full:
        start = spark.table("lake.hr.attendance").agg(F.min("att_date")).first()[0] or today
    else:
        start = today - timedelta(days=lookback_days)
    start = start.replace(day=1)
    nxt = date(today.year + (today.month == 12), today.month % 12 + 1, 1)
    end = nxt - timedelta(days=1)

    emp = spark.table("lake.hr.employees").select("emp_code", "department", "joined_on")
    att = spark.table("lake.hr.attendance").where(F.col("att_date").between(F.lit(start), F.lit(end)))
    cal = (spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS att_date")
           .where(F.dayofweek("att_date").between(2, 6))                       # Mon-Fri
           .withColumn("month", F.date_format("att_date", "yyyy-MM")))

    # employee x working day, so people with zero logs still count (LEFT JOIN below)
    grid = (F.broadcast(emp).crossJoin(F.broadcast(cal))
            .where(F.col("att_date") >= F.col("joined_on"))
            .join(att, ["emp_code", "att_date"], "left")
            .withColumn("present", F.when(F.col("status").isin(PRESENT),
                                          F.when(F.coalesce("half_day", F.lit(False)), 0.5).otherwise(1.0))
                        .otherwise(0.0)))

    z = lambda c: F.coalesce(F.col(c), F.lit(0))
    out = (grid.groupBy("department", "month").agg(
        F.countDistinct("emp_code").alias("headcount"),
        F.count("*").alias("working_days"),
        F.sum("present").alias("present_days"),
        F.sum(F.when(F.col("status") == "ABSENT", 1).otherwise(0)).alias("absent_days"),
        F.sum(F.when(F.col("status") == "LEAVE", 1).otherwise(0)).alias("leave_days"),
        F.sum(F.when(z("late_minutes") > 0, 1).otherwise(0)).alias("late_days"),
        F.sum(z("late_minutes")).alias("total_late_minutes"),
        F.sum(z("overtime_minutes")).alias("total_overtime_minutes"),
        F.round(F.sum(F.coalesce("work_hours", F.lit(0.0))), 2).alias("total_work_hours"))
        .withColumn("attendance_pct", F.round(F.col("present_days") / F.col("working_days") * 100, 2))
        .withColumn("computed_at", F.current_timestamp()))
    cols = spark.table("lake.hr.dept_monthly").columns
    out.select(*cols).writeTo("lake.hr.dept_monthly").overwritePartitions()      # only touched months


def maintain(spark):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    for t in ("hr.attendance", "hr.dept_monthly", "ops.alerts", "ops.job_runs"):
        strategy = "'sort'" if t == "hr.attendance" else "'binpack'"   # sort = follow the table's write order
        spark.sql(f"""CALL lake.system.rewrite_data_files(table => '{t}', strategy => {strategy},
                      options => map('min-input-files','5','delete-file-threshold','2'))""")
        spark.sql(f"CALL lake.system.rewrite_manifests('{t}')")
        spark.sql(f"CALL lake.system.expire_snapshots(table => '{t}', "
                  f"older_than => TIMESTAMP '{cutoff}', retain_last => 20)")
        spark.sql(f"CALL lake.system.remove_orphan_files(table => '{t}', older_than => TIMESTAMP '{cutoff}')")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("task", choices=["ingest", "marts", "maintain", "all"])
    ap.add_argument("--profile", default="etl", choices=["etl", "interactive", "alerts"])
    ap.add_argument("--lookback-days", type=int, default=45)
    ap.add_argument("--full", action="store_true")
    a = ap.parse_args()

    spark = build_session(a.task, a.profile)
    ensure_tables(spark)
    tasks = {
        "ingest": lambda: ingest(spark, a.lookback_days, a.full),
        "marts": lambda: marts(spark, a.lookback_days, a.full),
        "maintain": lambda: maintain(spark),
    }
    for name in (["ingest", "marts"] if a.task == "all" else [a.task]):
        with track_job(spark, name, JOB_SLA_SECONDS[name]):
            tasks[name]()
        print(f"{name}: done")
    spark.stop()


if __name__ == "__main__":
    main()
