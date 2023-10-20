-- CONNECT OR CREATE TO NECESSARY DATABASES
ATTACH 'duckdb/bronze_layer.db' AS bronze_layer;
USE bronze_layer;

-- CREATE AND POPULATE RAW EMPLOYEES TABLE
CREATE OR REPLACE TABLE bronze_layer.employees AS 
SELECT 
    * 
FROM 
    read_csv("data/employees.csv", auto_detect=true, all_varchar=true, header=true);
