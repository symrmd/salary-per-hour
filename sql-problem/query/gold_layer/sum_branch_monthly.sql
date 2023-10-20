-- CONNECT OR CREATE TO NECESSARY DATABASES
ATTACH 'duckdb/silver_layer.db' AS silver_layer;
ATTACH 'duckdb/gold_layer.db' AS gold_layer;
USE gold_layer;

-- CREATE TABLE SCHEMA
CREATE OR REPLACE TABLE gold_layer.sum_branch_monthly(
    year DATE,
    month DATE,
    branch_id INTEGER,
    total_work_duration_hour INT,
    average_monthly_salary_amount DOUBLE,
    total_monthly_salary_amount BIGINT,
    salary_per_hour_amount DOUBLE,
    total_employee_count INT,
    total_recruited_employee_count INT,
    total_resigned_employee_count INT
);

-- CREATE AGGREGATED TABLE FOR BRANCH MONTHLY SUMMARY
INSERT INTO gold_layer.sum_branch_monthly
SELECT
    DATE_TRUNC('YEAR', timesheet_date) AS year,
    DATE_TRUNC('MONTH', timesheet_date) AS month,
    branch_id,
    FLOOR(SUM(IFNULL(DATE_SUB('SECOND', timesheet_checkin_time, timesheet_checkout_time), 8*60*60)) / 60 / 60) AS total_work_duration_hour, -- if didn't checkout then use the default of 8 hours
    AVG(employee_monthly_salary_amount) AS average_monthly_salary_amount,
    SUM(employee_monthly_salary_amount) AS total_monthly_salary_amount,
    SUM(employee_monthly_salary_amount) /
    FLOOR(SUM(IFNULL(DATE_SUB('SECOND', timesheet_checkin_time, timesheet_checkout_time), 8*60*60)) / 60 / 60) AS salary_per_hour_amount,
    COUNT(DISTINCT employee_id) AS total_employee_count,
    COUNT(DISTINCT IF(DATE_TRUNC('MONTH', employee_join_date)=DATE_TRUNC('MONTH', timesheet_date),employee_id,NULL)) AS total_recruited_employee_count,
    COUNT(DISTINCT IF(DATE_TRUNC('MONTH', employee_resignation_date)=DATE_TRUNC('MONTH', timesheet_date),employee_id,NULL)) AS total_resigned_employee_count
FROM
    silver_layer.timesheets
INNER JOIN
    silver_layer.employees
USING (employee_id)
GROUP BY 1, 2, 3;
