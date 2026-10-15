# REVIEW.md

| # | Where | What is wrong | How you'd notice it | How I fixed it |
|---|---|---|---|---|
| 1 | `compute_late_minutes` | Uses `replace()` on whatever tz the datetime has (naive server/UTC), so shift start is not IST; negative values for early arrivals; ignores overnight shifts | Punch in 09:28 IST gives wrong late minutes; overnight punch-in 21:55 | Builds shift start as an IST-aware instant from the attendance date; floors; 0 inside grace or early |
| 2 | `compute_overtime` | Aware IST `end` minus naive Mongo `punch_out` raises `TypeError`; overnight end not on next day; no 30-min minimum | Punch-out for any employee returns 500 | All datetimes UTC-aware; end moves to next day for overnight; returns 0 under 30 min |
| 3 | `compute_work_hours` | Naive/aware mixing; no half-day rule | Same as above | Normalised to UTC; `half_day = hours < 4.5` |
| 4 | `MongoClient(...)` | Returns naive datetimes | Comparisons and ms conversion silently use local tz | `tz_aware=True`; all values go through `utc()` |
| 5 | `create_employee` | No validation (shift format, email, date); check-then-insert race; `created_at` is local time; returns datetime | POST with `shift_start="9am"`, or two concurrent posts | Pydantic validators; unique index + `DuplicateKeyError` -> 409; UTC; epoch ms |
| 6 | `list_employees` | `skip = page*page_size` skips the first page; `total` ignores the department filter; no sort; unbounded `page_size` | Page 1 empty; total wrong with filter | `(page-1)*page_size`, count uses same filter, sort by `emp_code`, `Query` bounds |
| 7 | `punch_in` | Unknown employee -> `None["shift_start"]` 500; `punched_at=0` treated as missing; `fromtimestamp` uses server tz; date not IST/overnight-aware; any `status` accepted (even ABSENT); race between `find_one` and `insert_one`; returns `id` and raw datetimes | Unknown `emp_code`; two simultaneous requests both 201 | 404; `is not None`; UTC; `attendance_day()` per R1; status validated; unique (emp_code,date) index with 409 on duplicate; response via serializer |
| 8 | `list_attendance` | Loads whole result set and sorts/paginates in Python; empty `date` dict possible; non-deterministic order; leaks `_id`; no validation of dates/status | Slow on large data; unstable pages | DB-side sort `(date desc, emp_code)`, skip/limit, `count_documents`, validation, serializer |
| 9 | Whole file | No indexes | `explain` shows COLLSCAN | Indexes created at startup (see DECISIONS.md) |
| 10 | Missing endpoints | punch-out, correction, analytics, explain not implemented | 404 | Implemented all |
| 11 | Analytics design | Risk of recomputing from punch times and of counting only employees that have logs | Headcount too low | Read stored values; department summary starts from `employees` |

**Looked at, not a defect:** the `> 10` late grace (matches samples: 35 -> 35, 5 -> 0); `ObjectId` never exposed; `status_code=201` on creates; IST as a fixed offset (India has no DST).
