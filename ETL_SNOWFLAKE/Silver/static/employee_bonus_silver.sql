CREATE OR REPLACE TABLE etl_snowflake.etl_silver.employee_bonus_silver (
    employee_id INT,
    bonus_year INT,
    annual_bonus NUMBER(10,2),
    performance_score NUMBER(3,1)
);

Insert into etl_snowflake.etl_silver.employee_bonus_silver
select 
employee_id,
bonus_year,
COALESCE(TRY_TO_NUMBER(TRIM(ANNUAL_BONUS)),0) AS annual_bonus,
ROUND(TRY_TO_NUMBER(TRIM(PERFORMANCE_SCORE))) AS performance_score1
from etl_snowflake.etl_bronze.employee_bonus_bronze_view;

select * from etl_snowflake.etl_silver.employee_bonus_silver;

select * from ETL_SNOWFLAKE.ETL_SILVER.EMPLOYEE_SILVER;



create or replace view ETL_SNOWFLAKE.ETL_SILVER.EMPLOYEE_SILVER_view 
as
select 
employee_id,
full_name,
email,
department,
hire_date,
salary,
annual_bonus,
start_date,
end_date,
is_active
from
ETL_SNOWFLAKE.ETL_SILVER.EMPLOYEE_SILVER
where is_active=TRUE
qualify row_number() over(partition by employee_id order by start_date desc) = 1
order by employee_id;
