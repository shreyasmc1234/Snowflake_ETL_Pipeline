create or replace database etl_snowflake;

USE DATABASE etl_snowflake;

create or replace schema etl_bronze;

use schema etl_bronze;

USE SCHEMA etl_snowflake.etl_bronze;

CREATE OR REPLACE TABLE  etl_snowflake.etl_bronze.employee_bronze
(
    employee_id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    department STRING,
    hire_date DATE,
    salary NUMBER(10,2),
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING
);
