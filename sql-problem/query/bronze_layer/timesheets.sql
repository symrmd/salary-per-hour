-- CONNECT OR CREATE TO NECESSARY DATABASES
ATTACH 'duckdb/bronze_layer.db' AS bronze_layer;
USE bronze_layer;

CREATE OR REPLACE TABLE bronze_layer.timesheets AS 
SELECT 
    * 
FROM 
    read_csv_auto("data/timesheets.csv", all_varchar=true, header=true);
