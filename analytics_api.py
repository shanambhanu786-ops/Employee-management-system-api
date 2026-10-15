"""
Serverless analytics API: API Gateway (HTTP API) -> Lambda -> Athena -> Iceberg tables in S3 (Glue catalog).

  GET /analytics/live/arrivals?hours=8              5-minute arrivals per department (stream output)
  GET /analytics/departments/summary?month=YYYY-MM  monthly department mart
  GET /analytics/leaderboard/late?month=&limit=10   late-minutes ranking, straight off the attendance table
  GET /analytics/stream/latency?minutes=15          streaming p95 / max lag (the boundary SLA, measured)

No servers to run, and cost is per query scanned. Controls: the Athena workgroup caps bytes scanned per query,
Iceberg partition pruning keeps scans small, and short-lived result reuse absorbs dashboard refresh storms.
Every value interpolated into SQL is validated first (ints in range, YYYY-MM regex, parsed dates).
"""
import json
import os
import re
import time
from datetime import date, timedelta

import boto3

athena = boto3.client("athena")
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "hr-analytics")
DATABASE = os.environ.get("GLUE_DATABASE", "hr")
MAX_PAGES = 5  # 5 x 1000 rows


class BadRequest(Exception):
    pass


class QueryFailed(Exception):
    pass


class QueryTimeout(Exception):
    pass


def _int(q, name, default, lo, hi):
    try:
        v = int(q.get(name, default))
    except (TypeError, ValueError):
        raise BadRequest(f"{name} must be an integer")
    if not lo <= v <= hi:
        raise BadRequest(f"{name} must be between {lo} and {hi}")
    return v


def _month(q):
    m = q.get("month") or date.today().strftime("%Y-%m")
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", m):
        raise BadRequest("month must be YYYY-MM")
    return m


def _bounds(month):
    y, m = map(int, month.split("-"))
    first = date(y, m, 1)
    return first, date(y + (m == 12), m % 12 + 1, 1) - timedelta(days=1)


def live_arrivals(q):
    hours = _int(q, "hours", 8, 1, 48)
    return ("SELECT to_unixtime(window_start) * 1000 AS window_start_ms, department, arrivals "
            f"FROM live_arrivals WHERE window_start >= date_add('hour', -{hours}, current_timestamp) "
            "ORDER BY window_start, department"), 1


def dept_summary(q):
    m = _month(q)
    return ("SELECT month, department, headcount, working_days, present_days, absent_days, leave_days, late_days, "
            "total_late_minutes, total_overtime_minutes, total_work_hours, attendance_pct, "
            "to_unixtime(computed_at) * 1000 AS computed_at_ms "
            f"FROM dept_monthly WHERE month = '{m}' ORDER BY department"), 5


def late_leaderboard(q):
    first, last = _bounds(_month(q))
    limit = _int(q, "limit", 10, 1, 100)
    return ("SELECT a.emp_code, e.name, e.department, sum(a.late_minutes) AS total_late_minutes, count(*) AS late_days "
            "FROM attendance a JOIN employees e ON e.emp_code = a.emp_code "
            f"WHERE a.att_date BETWEEN date '{first}' AND date '{last}' "
            "AND day_of_week(a.att_date) BETWEEN 1 AND 5 AND a.late_minutes > 0 "
            f"GROUP BY 1, 2, 3 ORDER BY 4 DESC, 1 ASC LIMIT {limit}"), 5


def stream_latency(q):
    minutes = _int(q, "minutes", 15, 1, 1440)
    return ("SELECT count(*) AS events, approx_percentile(lag_s, 0.95) AS p95_lag_s, max(lag_s) AS max_lag_s "
            f"FROM attendance_events WHERE event_time >= date_add('minute', -{minutes}, current_timestamp)"), 1


ROUTES = {
    "/analytics/live/arrivals": live_arrivals,
    "/analytics/departments/summary": dept_summary,
    "/analytics/leaderboard/late": late_leaderboard,
    "/analytics/stream/latency": stream_latency,
}


def cast(v, t):
    if v is None:
        return None
    if t in ("tinyint", "smallint", "integer", "bigint"):
        return int(v)
    if t in ("double", "float", "real", "decimal"):
        return float(v)
    if t == "boolean":
        return v == "true"
    return v


def run_query(sql, max_age_minutes, context):
    qid = athena.start_query_execution(
        QueryString=sql, QueryExecutionContext={"Database": DATABASE}, WorkGroup=WORKGROUP,
        ResultReuseConfiguration={"ResultReuseByAgeConfiguration": {"Enabled": True, "MaxAgeInMinutes": max_age_minutes}},
    )["QueryExecutionId"]
    delay = 0.3
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if status["State"] == "SUCCEEDED":
            break
        if status["State"] in ("FAILED", "CANCELLED"):
            raise QueryFailed(status.get("StateChangeReason", status["State"]))
        if context.get_remaining_time_in_millis() < 3000:
            raise QueryTimeout(qid)
        time.sleep(delay)
        delay = min(delay * 1.5, 2)

    cols, rows, token = None, [], None
    for _ in range(MAX_PAGES):
        kw = {"QueryExecutionId": qid, "MaxResults": 1000}
        if token:
            kw["NextToken"] = token
        page = athena.get_query_results(**kw)
        rs = page["ResultSet"]
        if cols is None:  # only the first page carries the header row
            cols = [(c["Label"], c["Type"]) for c in rs["ResultSetMetadata"]["ColumnInfo"]]
            data = rs["Rows"][1:]
        else:
            data = rs["Rows"]
        rows += [[v.get("VarCharValue") for v in r["Data"]] for r in data]
        token = page.get("NextToken")
        if not token:
            break
    return cols, rows


def respond(status, body, max_age=0):
    headers = {"Content-Type": "application/json"}
    if max_age:
        headers["Cache-Control"] = f"max-age={max_age * 60}"
    return {"statusCode": status, "headers": headers, "body": json.dumps(body)}


def handler(event, context):
    path = (event.get("rawPath") or "").rstrip("/")
    route = ROUTES.get(path)
    if not route:
        return respond(404, {"error": "not found", "routes": sorted(ROUTES)})
    try:
        sql, max_age = route(event.get("queryStringParameters") or {})
        cols, rows = run_query(sql, max_age, context)
    except BadRequest as exc:
        return respond(400, {"error": str(exc)})
    except QueryTimeout as exc:
        return respond(504, {"error": "query still running; retry shortly", "query_execution_id": str(exc)})
    except QueryFailed as exc:
        return respond(502, {"error": str(exc)})
    items = [{n: cast(v, t) for (n, t), v in zip(cols, r)} for r in rows]
    return respond(200, {"count": len(items), "items": items}, max_age)
