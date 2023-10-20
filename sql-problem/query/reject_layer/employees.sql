-- CONNECT OR CREATE TO NECESSARY DATABASES
ATTACH 'duckdb/bronze_layer.db' AS bronze_layer;
ATTACH 'duckdb/reject_layer.db' AS reject_layer;
USE reject_layer;

-- CREATE OR REPLACE TABLE SCHEMA
CREATE OR REPLACE TABLE reject_layer.employees(
    employe_id VARCHAR,
    branch_id VARCHAR,
    salary VARCHAR,
    join_date VARCHAR,
    resign_date VARCHAR,
    rejected_reason VARCHAR
);

-- CREATE MACROS FOR DATE VALIDATION
CREATE OR REPLACE MACRO reject_layer.validate_date(date) AS 
REGEXP_MATCHES(date, '\d{4}[-\\](0?[1-9]|1[0-2])[-\\](0?[1-9]|[12][0-9]|3[01])')
OR
REGEXP_MATCHES(date, '(0?[1-9]|[12][0-9]|3[01])[-\\](0?[1-9]|1[0-2])[-\\]\d{4}')
OR
REGEXP_MATCHES(date, '(0?[1-9]|[12][0-9]|3[01])[-\\](Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[-\\]\d{4}');

-- INSERTING ROWS TO REJECT
INSERT INTO reject_layer.employees
WITH employee_id_with_duplicates AS (
    SELECT 
        employe_id 
    FROM 
        bronze_layer.employees 
    GROUP BY 1 
    HAVING COUNT(*) > 1
)
SELECT
    employe_id,
    branch_id,
    salary,
    join_date,
    resign_date,
    CASE
        WHEN REGEXP_MATCHES(salary, '[0-9]+') AND salary < 0 THEN 'INVALID SALARY RANGE'
        WHEN employe_id IN (SELECT employe_id FROM employee_id_with_duplicates) THEN 'DUPLICATE EMPLOYEE ID'
        ELSE 'DATA TYPE MISMATCH'
    END AS rejected_reason
FROM
    bronze_layer.employees
WHERE
    -- data type mismatch
    NOT REGEXP_MATCHES(employe_id, '[0-9]+')
    OR NOT REGEXP_MATCHES(branch_id, '[0-9]+')
    OR NOT REGEXP_MATCHES(salary, '[0-9]+')
    OR NOT validate_date(join_date)
    OR NOT validate_date(resign_date)
    
    -- logical rejection
    OR REGEXP_MATCHES(salary, '[0-9]+') AND salary < 0
    OR employe_id IN (SELECT employe_id FROM employee_id_with_duplicates);
