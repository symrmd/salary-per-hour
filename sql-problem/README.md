<!-- TABLE OF CONTENTS -->
## Table of Contents
<ol>
<li>
    <a href="#about-the-project">About The Project</a>
</li>
<li>
    <a href="#why-duckdb">Why Duck DB</a>
    <ul>
    <li><a href="#features">Features</a></li>
    <li><a href="#comparison-with-production-ready-dbs">Comparison with Production Ready DBs</a></li>
    <li><a href="#alternatives">Alternatives</a></li>
    </ul>
</li>
<li>
    <a href="#getting-started">Getting Started</a>
    <ul>
    <li><a href="#installation-windows">Installation (Windows)</a></li>
    </ul>
</li>
<li><a href="#code-layout">Code Layout</a></li>
<li>
    <a href="#database-schemas">Database Schemas</a>
    <ul>
    <li><a href="#bronze-layer">Bronze Layer</a></li>
    <li><a href="#reject-layer">Reject layer</a></li>
    <li><a href="#silver-layer">Silver layer</a></li>
    <li><a href="#gold-layer">Gold layer</a></li>
    </ul>
</li>
<li><a href="#steps-of-execution">Steps of Execution</a></li>
<li><a href="#scheduling">Scheduling</a></li>
</ol>



<!-- ABOUT THE PROJECT -->
## About The Project

This specific section of the repository was made to accomplish the SQL part of the Salary per Hour code challenge. Every frameworks used will be open source to enable local installation. Since everything is meant to be processed on top of SQL, for this problem this is the framework used:

| Framework                         | Version | Installation Command    | Docs            |
| --------------------------------- | ------- | ----------------------- | --------------- |
| `DuckDB`                          | `0.9.1` | [DuckDB CLI][duckdb-cli-install]   | [DuckDB Docs][duckdb-url] |

<p align="right">(<a href="#readme-top">back to top</a>)</p>



## Why DuckDB

### Features
`DuckDB` was chosen because of its inherently OLAP features. It uses columnar format for its tables and a lot of operations are done via vectorizations, while also being built to handle larger-than-memory data. 

### Comparison with Production Ready DBs
These features that `DuckDB` has makes it (in my opinion) perfect as a framework that is basically the smaller and local version of the OLAP databases that companies use in production environment. Making switching context easier since the core concepts will be the same.

### Alternatives
The use of standard relational DBs like `PostgreSQL` and `MySQL` is a good alternative as they are more mature of a product compared to `DuckDB`, though some performance sacrifices will be unavoidable for relatively heavy analytical reads. For production environment with high volume of data and respectively complex queries, distributed OLAP Databases are preferred (e.g `BigQuery`, `Databricks`, `Clickhouse`).

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- GETTING STARTED -->
## Getting Started

This section details the steps needed to run the program in the repository.

### Installation (Windows)

1. Download the `DuckDB` executable [DuckDB CLI][duckdb-cli-install]
2. Add the executable path to your environment variables
3. verify `DuckDB` installation
   ```sh
   duckdb --version
   ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CODE LAYOUT -->
## Code Layout

The directory layout of the repository and each respective purposes

* __data/__

  Contains input data in the format of CSV.

* __duckdb/__

  Contains the files maintained by DuckDB to persistently store the data ingested. Each file is a different database schema.

* __playground/__

  Contains the queries that is meant for exploring and trying out results.

* __query/__

  Contains the queries that can be executed for each database schema, though the SQL script has been made so we can execute it anywhere (e.g executing bronze_layer/employees.sql from silver_layer).

* __ingest.sh__

  Is a bash script that completes the task as a whole, from ingestion, cleaning, transformation, to aggregation. Though it is preferred to use a scheduler dependency features instead of putting all different process in a singular script.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- DATABASE SCHEMAS -->
## Database Schemas

Explanation of each database schema functionalities

### Bronze Layer

`employees` table:

| column_name | column_type |
|-------------|-------------|
| employe_id  | VARCHAR     |
| branch_id   | VARCHAR     |
| salary      | VARCHAR     |
| join_date   | VARCHAR     |
| resign_date | VARCHAR     |

`timesheets` table:

| column_name  | column_type |
|--------------|-------------|
| timesheet_id | VARCHAR     |
| employee_id  | VARCHAR     |
| date         | VARCHAR     |
| checkin      | VARCHAR     |
| checkout     | VARCHAR     |

In this layer, the rawest version of an ingestion occurs. It serves as sort of a data lake and these following rules apply:

* No type conversions to minimalize failures, hence the varchar data types.

By implementing ELT, I believe we can gain benefits such as:
* Easy tracebacks and debugging when further layer fails or experiencing logic errors.

Ideally, an ingestion layer should have:
* Extensive data management features, such as storing and querying historical versions of a table.
* High pipeline reliability or very low risk of failure.

### Reject Layer

`employees` table:

|   column_name   | column_type |
|-----------------|-------------|
| employe_id      | VARCHAR     |
| branch_id       | VARCHAR     |
| salary          | VARCHAR     |
| join_date       | VARCHAR     |
| resign_date     | VARCHAR     |
| rejected_reason | VARCHAR     |

`timesheets` table:

|   column_name   | column_type |
|-----------------|-------------|
| timesheet_id    | VARCHAR     |
| employee_id     | VARCHAR     |
| date            | VARCHAR     |
| checkin         | VARCHAR     |
| checkout        | VARCHAR     |
| rejected_reason | VARCHAR     |

In this layer, rows that are deemed invalid are ingested to tables with the same name (just different DB schema). These following rules apply:

* Using regex, reject records with columns that values doesn't match with expected data types
* Using groupby, reject records with columns that have duplicates based on the expected primary key
* Using value comparison, reject records with columns that values doesn't match with expected domain-specific logic (e.g salary below 0)

In my opinion, By implementing a rejection layer, we're able to:
* Decrease the chance of transformation layer failure by filtering out broken data.
* Decrease the chance of logical fallacy in further layer by filtering out dirty data.

A rejection layer is not necessary, In the end it's up to the users whether or not they would prefer:
* Option A: Raise failures on pipelines that were caused by dirty data, alerting the users and stopping further layers from running until it's fixed.
* Option B: Avoid and continue with further layers' pipelines while collecting the data of what records have been rejected and inform the users of the rejected data.

### Silver Layer

`employees` table:

|          column_name           | column_type |
|--------------------------------|-------------|
| employee_id                    | INTEGER     |
| branch_id                      | INTEGER     |
| employee_monthly_salary_amount | BIGINT      |
| employee_join_date             | DATE        |
| employee_resignation_date      | DATE        |

`timesheets` table:

|       column_name       | column_type |
|-------------------------|-------------|
| timesheet_id            | INTEGER     |
| employee_id             | INTEGER     |
| timesheet_date          | DATE        |
| timesheet_checkin_time  | TIME        |
| timesheet_checkout_time | TIME        |

In this layer, rows are going to be type casted to expected data types and renamed to a more informative ones. These following rules apply:

* By using the data ingested on reject layer, exclude the records that are detected to be faulty.
* Type cast from VARCHAR to expected data type, including date string with different formats.

The goal of implementing this layer is to:
* Easily query clean version of a source data.
* Easily understand schema, behavior, and relationship of all tables from different sources through conformed column names and informative descriptions.

### Gold Layer

`sum_branch_monthly` table:

|          column_name           | column_type |
|--------------------------------|-------------|
| year                           | DATE        |
| month                          | DATE        |
| branch_id                      | INTEGER     |
| total_work_duration_hour       | INTEGER     |
| average_monthly_salary_amount  | DOUBLE      |
| total_monthly_salary_amount    | BIGINT      |
| salary_per_hour_amount         | DOUBLE      |
| total_employee_count           | INTEGER     |
| total_recruited_employee_count | INTEGER     |
| total_resigned_employee_count  | INTEGER     |

In this layer, new tables are going to be created based on the business logics and relationships from silver layer tables. These tables will have metrices and in general follows the usual DWH design or is an aggregation above that.

The goal of implementing this layer is to avoid:
* Stakeholders using too much joins or arranging complex queries to obtain metrices.
* Having to know too many function specific tables and uneeded columns to obtain metrices.

The table created in this layer is what will be the end product of this challenge.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- Steps of Executions -->
## Steps of Executions

This section will document the steps of executions needed to accomplish the challenge/task.

### 1. Delete `duckdb/` Contents

Since we want to simulate the process from the very beginning, let's delete the content of `duckdb/` so there will be no data and schemas.

### 2. CSV Simple Ingestion (Bronze Layer)

This step of the process requires us to run two CLI commands:
1. Ingest `employees.csv`
```
duckdb -no-stdin -init query/bronze_layer/employees.sql
```
2. Ingest `timesheets.csv`
```
duckdb -no-stdin -init query/bronze_layer/timesheets.sql
```
3. As a way to test the idempotency, you can run it two times and check the result using simple `count(*)`. You can connect by calling this command
```
duckdb duckdb/bronze_layer.db
```
In theory, the SQL script should be idempotent as it's performing an overwrite by recreating new tables.

### 3. Reject Invalid Records (Reject Layer)

1. Reject `bronze_layer.employees`
```
duckdb -no-stdin -init query/reject_layer/employees.sql
```
2. Reject `bronze_layer.timesheets`
```
duckdb -no-stdin -init query/reject_layer/timesheets.sql
```
3. (Optional) check rejected records
```
duckdb -readonly -no-stdin -init playground/get_rejected_employee.sql duckdb/reject_layer.db
```
| employe_id |    rejected_reason    |
|------------|-----------------------|
| 218078     | DUPLICATE EMPLOYEE ID |
| 218078     | DUPLICATE EMPLOYEE ID |

Same as bronze layer, in theory, the SQL script should be idempotent as it's performing an overwrite by recreating new tables.

### 4. Clean Data and Simple Transformation (Silver Layer)

1. Clean `bronze_layer.employees`, avoiding `reject_layer.employees`
```
duckdb -no-stdin -init query/silver_layer/employees.sql
```
2. Clean `bronze_layer.timesheets`, avoiding `reject_layer.timesheets`
```
duckdb -no-stdin -init query/silver_layer/timesheets.sql
```
Same as previous layers, in theory, the SQL script should be idempotent as it's performing an overwrite by recreating new tables.

### 5. Transform and Aggregate Data (Gold Layer)

1. Create a new aggregated table `gold_layer.sum_branch_monthly` based on `silver_layer.employees` and `silver_layer.timesheets`
```
duckdb -no-stdin -init query/gold_layer/sum_branch_monthly.sql
```
2. Try and query the result table using the `playground/get_salary_per_hour.sql`
```
duckdb -readonly -no-stdin -init playground/get_salary_per_hour.sql duckdb/gold_layer.db
```
Same as previous layers, in theory, the SQL script should be idempotent as it's performing an overwrite by recreating new tables.


<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTRIBUTING -->
## Scheduling

Regarding scheduling, idempotency should be made possible because of the nature of the SQL scripts that have been created. To have a more granular control of dependencies, I personally prefer to not use `ingest.sh` and instead opt to use Airflow `BashOperator` with multiple tasks.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



[duckdb-url]: https://duckdb.org/docs/ 
[duckdb-cli-install]: https://duckdb.org/docs/installation/index