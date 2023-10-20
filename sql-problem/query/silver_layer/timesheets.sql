-- CONNECT OR CREATE TO NECESSARY DATABASES
ATTACH 'duckdb/bronze_layer.db' AS bronze_layer;
ATTACH 'duckdb/reject_layer.db' AS reject_layer;
ATTACH 'duckdb/silver_layer.db' AS silver_layer;
USE silver_layer;

-- CREATE TABLE SCHEMA
CREATE OR REPLACE TABLE silver_layer.timesheets(
    timesheet_id INTEGER,
    employee_id INTEGER,
    timesheet_date DATE,
    timesheet_checkin_time TIME,
    timesheet_checkout_time TIME
);

-- INGEST CLEAN TIMESHEETS DATA
INSERT INTO silver_layer.timesheets
SELECT
    CAST(timesheets.timesheet_id AS INTEGER) AS timesheet_id,
    CAST(timesheets.employee_id AS INTEGER) AS employee_id,
    CAST(timesheets.date AS DATE) AS timesheet_date,
    CAST(timesheets.checkin AS TIME) AS timesheet_checkin_time,
    CAST(timesheets.checkout AS TIME) AS timesheet_checkout_time
FROM
    bronze_layer.timesheets AS timesheets
LEFT JOIN
    reject_layer.timesheets AS rejected_timesheets
ON timesheets.timesheet_id = rejected_timesheets.timesheet_id
WHERE
    rejected_timesheets.timesheet_id IS NULL;
