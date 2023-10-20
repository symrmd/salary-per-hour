-- CONNECT OR CREATE TO NECESSARY DATABASES
ATTACH 'duckdb/bronze_layer.db' AS bronze_layer;
ATTACH 'duckdb/reject_layer.db' AS reject_layer;
USE reject_layer;

-- CREATE OR REPLACE TABLE SCHEMA
CREATE OR REPLACE TABLE reject_layer.timesheets(
    timesheet_id VARCHAR,
    employee_id VARCHAR,
    date VARCHAR,
    checkin VARCHAR,
    checkout VARCHAR,
    rejected_reason VARCHAR
);

-- CREATE MACROS FOR DATE VALIDATION
CREATE OR REPLACE MACRO reject_layer.validate_date(date) AS 
REGEXP_MATCHES(date, '\d{4}[-\\](0?[1-9]|1[0-2])[-\\](0?[1-9]|[12][0-9]|3[01])')
OR
REGEXP_MATCHES(date, '(0?[1-9]|[12][0-9]|3[01])[-\\](0?[1-9]|1[0-2])[-\\]\d{4}')
OR
REGEXP_MATCHES(date, '(0?[1-9]|[12][0-9]|3[01])[-\\](Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[-\\]\d{4}');

-- CREATE MACROS FOR TIME VALIDATION
CREATE OR REPLACE MACRO reject_layer.validate_time(time) AS 
REGEXP_MATCHES(time, '(0?[0-9]|1[0-9]|2[0-4]):(0?[0-9]|[1-5][0-9]):(0?[0-9]|[1-5][0-9])');

-- INSERTING ROWS TO REJECT
INSERT INTO reject_layer.timesheets
WITH timesheet_id_with_duplicates AS (
    SELECT 
        timesheet_id 
    FROM 
        bronze_layer.timesheets 
    GROUP BY 1 
    HAVING COUNT(*) > 1
)
SELECT
    timesheet_id,
    employee_id,
    date,
    checkin,
    checkout,
    CASE
        WHEN timesheet_id IN (SELECT timesheet_id FROM timesheet_id_with_duplicates) THEN 'DUPLICATE EMPLOYEE ID'
        ELSE 'DATA TYPE MISMATCH'
    END AS rejected_reason
FROM
    bronze_layer.timesheets
WHERE
    -- data type mismatch
    NOT REGEXP_MATCHES(timesheet_id, '[0-9]+')
    OR NOT REGEXP_MATCHES(employee_id, '[0-9]+')
    OR NOT validate_date(date)
    OR NOT validate_time(checkin)
    OR NOT validate_time(checkout)
    
    -- duplicate rejection
    OR timesheet_id IN (SELECT timesheet_id FROM timesheet_id_with_duplicates);
