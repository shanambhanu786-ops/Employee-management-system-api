# DECISIONS.md

1. **Indexes.** `employees.emp_code` unique (identity, 409 on duplicates). `attendance_logs (emp_code, date)` unique: one record per day, per-employee range scans, and the `$lookup` in analytics. `attendance_logs (date desc, emp_code)`: default listing order and month scans for the leaderboard. `employees (department, emp_code)`: filtered listing. Rejected: an index on `status`, because it has five values and filters little.
2. **Punch-in race.** Both requests may pass the friendly `find_one` check. Both try `insert_one`; the unique index lets exactly one win. The other gets `DuplicateKeyError`, which I turn into 409. Nothing is written twice.
3. **Ties.** Sorted by late minutes descending, then `emp_code` ascending, then `limit`. The cutoff is therefore stable between calls, though one tied person can be left out. Tied people share a `rank`.
4. **Headcount.** The summary pipeline starts from `employees` and `$lookup`s logs, so someone with no logs still adds 1 to headcount and their working days to the denominator.
5. **Change at 100x.** Pre-aggregate monthly department numbers (the Spark job already does this) and serve the API from them instead of a live `$lookup` per request.
