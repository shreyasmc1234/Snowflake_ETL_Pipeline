USE SCHEMA etl_snowflake.etl_silver;

CREATE OR REPLACE TABLE etl_snowflake.etl_silver.employee_silver (
    employee_id INT,
    first_name STRING,
    last_name STRING,
    full_name STRING,
    email STRING,
    department STRING,
    hire_date DATE,
    salary NUMBER(10,2),
    annual_bonus NUMBER(10,2),
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_active BOOLEAN
);
