<!-- TABLE OF CONTENTS -->
## Table of Contents
<ol>
<li>
    <a href="#about-the-project">About The Project</a>
</li>
<li>
    <a href="#why-polars">Why Polars</a>
    <ul>
    <li><a href="#features">Features</a></li>
    <li><a href="#comparison-with-production-ready-dbs">Comparison with Production Ready DBs</a></li>
    </ul>
</li>
<li>
    <a href="#why-deltalake">Why Deltalake</a>
</li>
<li>
    <a href="#getting-started">Getting Started</a>
    <ul>
    <li><a href="#prerequisites">Prerequisites</a></li>
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

This specific section of the repository was made to accomplish the Python part of the Salary per Hour code challenge. Every frameworks used will be open source to enable local installation. These are the language and frameworks used:
| Language                         | Version    
| --------------------------------- | ----------------------- 
| `Python`                          | `3.9`    

| Framework        | Version | Installation Command    | Docs            |
| ---------------- | ------- | ----------------------- | --------------- |
| `Polars`         | `0.19.8` |  `pip install polars==0.19.8` | [Polars Docs][polars-url] |
| `deltalake`      | `0.12.0` |  `pip install deltalake==0.12.0` | [Deltalake Docs][delta-rs-url] |

<p align="right">(<a href="#readme-top">back to top</a>)</p>


## Why Polars

### Features
`Polars` was chosen because of its very effective way of computing large & complex analytical queries, able to handle larger than memory processing by leveraging streaming and lazy evaluations. When compared to frameworks like `pandas`, `polars` will almost always be faster because of the `Arrow` usage, lazy evaluation, or streaming alltogehter. Since ML libraries are not needed, things that exclusively make pandas great won't be taken into consideration.

### Comparison with Production Ready Frameworks
In some production cases when we're dealing with huge load of I/Os and not so much on the computing side, the use of `polars` can save the cost and time complexity of building a distributed computing system in the first place. Although, there will definitely be a point where pipelines will be complex enough through sheer volume or logical complexity, that it requires us to use distributed system like `Spark`.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


## Why Deltalake

### Features
`Deltalake` has recently started the development of `delta-rs` which is a development on top of `Rust` with `Python` bindings, making it easier to setup locally. By using `parquet` as its file format, being `ACID`, while also having detailed granular features like `UPSERT` and time travel, it makes `Deltalake` a perfect tool to make raw data OLAP-ready.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- GETTING STARTED -->
## Getting Started

This section details the steps needed to run the program in the repository.

### Prerequisites

* `Python`, make sure it is installed
  ```sh
  python --version
  ```

### Installation

1. (Optional) use python `virtualenv`
   ```ps
   pip install virtualenv==20.0.35
   virtualenv env
   env\Scripts\activate #windows specific command
   ```
2. Install the libraries needed
   ```sh
   pip install -r requirements.txt
   ```
2. Confirm it's installed
   ```sh
   metl --help
   ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CODE LAYOUT -->
## Code Layout

The directory layout of the repository and each respective purposes

* __cli/__

  Contains the CLI configuration python script that `setup.py` will read. The CLI will be used to parameterized ingestion commands.

* __data/__

  Contains input data in the format of CSV.

* __deltalake/__

  Contains the files maintained by `deltalake` to persistently store the data ingested. Each folder is a different layer and for each layer has different folders for different tables stored. Those folder then contains the open table format of `deltalake`.

* __duckdb/__

  Contains the files maintained by `duckdb` to persistently store the `gold_layer` table.

* __models/__

  Contains the python scripts that are required to run the pipelines. All transformation logics, extraction, and loading will be in this folder.

* __tests/__

  Contains unit tests for transformations done in the python scripts.

* __setup.sh__

  script to install local CLI app by using contents on `cli/`.

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
* Since the task requires an incremental pipeline, this layer will implement a `DELETE+INSERT` operation based on `employees.join_date` or `timesheets.date`. That should make it idempotent.

By implementing ELT, I believe we can gain benefits such as:
* Easy tracebacks and debugging when further layer fails or experiencing logic errors.

By storing raw layer on deltalake, it's possible to:
* store, query, or restore historical versions of a table.

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

* Using error handling, reject records with columns that values can't be cast to expected data types
* Using vectorization, reject records with columns that have duplicates based on the expected primary key
* Using value comparison, reject records with columns that values doesn't match with expected domain-specific logic (e.g salary below 0)
* Same as the previous layer, this layer will implement an idempotent `DELETE+INSERT` operation based on `employees.join_date` or `timesheets.date`.

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
| timesheet_checkin_datetime  | DATETIME        |
| timesheet_checkout_datetime | DATETIME        |

In this layer, rows are going to be type casted to expected data types and renamed to a more informative ones. These following rules apply:

* By using the data ingested on reject layer, exclude the records that are detected to be faulty.
* Type cast from VARCHAR to expected data type, including date string with different formats.
* Change checkin and checkout to `DATETIME` and make it so if checkout is less than checkin, it will be appended by `1 DAY`.
* Since at this layer, I believe it's secure enough to trust that broken primary key constraints will be handled by the reject layer, this layer will implement an idempotent `UPSERT` operation based on `employees.employee_id` or `timesheets.timesheet_id`.

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

Although the task requires us to make this pipeline increments daily, since the nature of the request is monthly metrices, with data that is also monthly only (e.g employee's salary), this pipeline will instead increment in a monthly behavior, using `DELETE+INSERT` to overwrite current month.

This table will also ingest to a local `DuckDB` file (located in `duckdb/gold_layer.db`). The table created in this layer is what will be the end product of this challenge.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- Steps of Executions -->
## Steps of Executions

This section will document the steps of executions needed to accomplish the challenge/task.

### 1. Delete `deltalake/` Contents

Since we want to simulate the process from the very beginning, let's delete the content of `deltalake/` so there will be no data and schemas.

### Mini ETL CLI Context

```
Usage: metl ingest [OPTIONS]

  Ingestion Command for Mini ETL App

Options:
  -l, --layer_name TEXT   Layer name, consist of bronze_layer, reject_layer,
                          silver_layer, & gold_layer  [required]
  -m, --model_name TEXT   Model name, the name of the folder inside every
                          layers' folder  [required]
  -st, --start_time TEXT  Start Date or Timestamp, Use ISO Format
  -f, --full_refresh      Whether or not the ingestion is full refresh or
                          incremental
  --help                  Show this message and exit.
```

### 2. CSV Simple Ingestion (Bronze Layer)

This step of the process requires us to run two CLI commands:
1. Ingest `employees.csv` incrementally
```
metl ingest --layer_name bronze_layer --model_name employees --start_time 2023-10-20
```
2. Alternatively, full refresh `employees.csv`
```
metl ingest --layer_name bronze_layer --model_name employees --full_refresh
```
3. Ingest `timesheets.csv` incrementally or full refresh
```
metl ingest --layer_name bronze_layer --model_name employees --start_time 2023-10-20
```
4. As a way to test the idempotency, you can run it multiple times and check the result row counts. You can execute a simple cli query to check the shape of the table
```
metl check --layer_name bronze_layer --model_name employees
```
In theory, the SQL script should be idempotent as it's performing an overwrite by delete and inserting.

### 3. Reject Invalid Records (Reject Layer)

1. Reject `bronze_layer.employees` incrementally or full refresh
```
metl ingest --layer_name reject_layer --model_name employees --full_refresh
```
2. Reject `bronze_layer.timesheets`
```
metl ingest --layer_name reject_layer --model_name timesheets --start_time 2023-10-20
```
3. (Optional) check rejected records
```
metl check --layer_name reject_layer --model_name employees
```
| employe_id |    rejected_reason    |
|------------|-----------------------|
| 218078     | DUPLICATE EMPLOYEE ID |
| 218078     | DUPLICATE EMPLOYEE ID |

Same as bronze layer, in theory, the SQL script should be idempotent as it's performing an overwrite by delete insert operation.

### 4. Clean Data and Simple Transformation (Silver Layer)

1. Clean `bronze_layer.employees`, avoiding `reject_layer.employees`
```
metl ingest --layer_name silver_layer --model_name employees --start_time 2023-10-20
```
2. Clean `bronze_layer.timesheets`, avoiding `reject_layer.timesheets`
```
metl ingest --layer_name silver_layer --model_name timesheets --full_refresh
```
Same as previous layers, in theory, the SQL script should be idempotent as it's performing an `UPSERT`.

### 5. Transform and Aggregate Data (Gold Layer)

1. Create a new aggregated table `gold_layer.sum_branch_monthly` based on `silver_layer.employees` and `silver_layer.timesheets`
```
metl ingest --layer_name gold_layer --model_name sum_branch_monthly --full_refresh
```
2. Try and query the result table using the `playground/get_salary_per_hour.sql`
```
metl check --layer_name gold_layer --model_name sum_branch_monthly
```
Same as previous layers, in theory, the SQL script should be idempotent as it's performing an delete insert.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- SCHEDULING -->
## Testing

Running testing should be as simple as 
```
python -m unittest
```
It will execute the whole `test_*.py` file inside the `test/` directory

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- SCHEDULING -->
## End Result
The end result is both a `deltalake` open table format and a `DuckDB` file. Usually, the `ETL` will push the data to the data warehouse, or in the case of open table format, create a connector from a data warehouse to the Cloud Storage location. For the sake of simulating end user's `SQL` usage, Here's how to use `DuckDB`

### Prerequisites
The `DuckDB` executable [DuckDB CLI][duckdb-cli-install] must be installed and put on the `PATH` directory

### Connecting to DuckDB
```
duckdb -readonly duckdb/gold_layer.db
```

### Querying Data
```
SELECT year, month, salary_per_hour_amount FROM sum_branch_month;
```


<!-- SCHEDULING -->
## Scheduling

Regarding scheduling, idempotency should be made possible because of the nature of the SQL scripts that have been created. Using the CLI created also makes it more possible to control dependencies should new tables come in the future.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



[polars-url]: https://pola-rs.github.io/polars/py-polars/html/index.html 
[delta-rs-url]: https://delta-io.github.io/delta-rs/python/
[duckdb-cli-install]: https://duckdb.org/docs/installation/index