-- CONNECT OR CREATE TO NECESSARY DATABASES
ATTACH 'duckdb/bronze_layer.db' AS bronze_layer;
ATTACH 'duckdb/reject_layer.db' AS reject_layer;
ATTACH 'duckdb/silver_layer.db' AS silver_layer;
USE silver_layer;

-- CREATE TABLE SCHEMA
CREATE OR REPLACE TABLE silver_layer.employees(
    employee_id INTEGER,
    branch_id INTEGER,
    employee_monthly_salary_amount BIGINT,
    employee_join_date DATE,
    employee_resignation_date DATE
);

-- INGEST CLEAN EMPLOYEES DATA
INSERT INTO silver_layer.employees
SELECT
    CAST(employees.employe_id AS INTEGER) AS employee_id,
    CAST(employees.branch_id AS INTEGER) AS branch_id,
    CAST(employees.salary AS BIGINT) AS employee_monthly_salary_amount,
    CAST(employees.join_date AS DATE) AS employee_join_date,
    CAST(employees.resign_date AS DATE) AS employee_resignation_date
FROM
    bronze_layer.employees AS employees
LEFT JOIN
    reject_layer.employees AS rejected_employees 
ON employees.employe_id = rejected_employees.employe_id
WHERE
    rejected_employees.employe_id IS NULL;
