#!/bin/bash

# ingest bronze layer data
duckdb -no-stdin -init query/bronze_layer/employees.sql && \
duckdb -no-stdin -init query/bronze_layer/timesheets.sql && \

# ingest reject layer data
duckdb -no-stdin -init query/reject_layer/employees.sql && \
duckdb -no-stdin -init query/reject_layer/timesheets.sql && \

# ingest silver layer data
duckdb -no-stdin -init query/silver_layer/employees.sql && \
duckdb -no-stdin -init query/silver_layer/timesheets.sql && \

# ingest gold layer data
duckdb -no-stdin -init query/gold_layer/sum_branch_monthly.sql