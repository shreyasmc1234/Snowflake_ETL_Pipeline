-- Implementing scd2 to handle full data 

Create or replace procedure  etl_snowflake.etl_silver.employee_silver_history()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE curr_ts timestamp;
BEGIN

curr_ts := current_timestamp();


MERGE INTO etl_snowflake.etl_silver.employee_silver a
USING  etl_snowflake.etl_bronze.stream1 b
on a.employee_id=b.employee_id and a.is_active=TRUE
WHEN MATCHED and b.METADATA$ACTION='DELETE' AND b.METADATA$ISUPDATE=TRUE
THEN
UPDATE SET a.is_active=FALSE, a.end_date =: curr_ts

WHEN NOT MATCHED and b.METADATA$ACTION='INSERT' AND b.METADATA$ISUPDATE=FALSE
THEN
INSERT (a.employee_id,
a.first_name,
a.last_name,
a.full_name,
a.email,
a.department,
a.hire_date,
a.salary,
a.annual_bonus,
a.start_date,
a.end_date,
a.is_active) 
values(
b.employee_id,
b.first_name,
b.last_name,
b.first_name||' '||b.last_name,
b.email,
b.department,
b.hire_date,
b.salary,
b.salary*(10/100),
:curr_ts,
NULL,
TRUE
);

INSERT INTO etl_snowflake.etl_silver.employee_silver 
Select b.employee_id,
b.first_name,
b.last_name,
b.first_name||' '||b.last_name,
b.email,
b.department,
b.hire_date,
b.salary,
b.salary*(10/100),
:curr_ts,
NULL,
TRUE from etl_snowflake.etl_bronze.stream2 b
where b.METADATA$ACTION='INSERT' and b.METADATA$ISUPDATE=TRUE;

Return 'History Procedure Executed successfully';




---------------- View creation to handle Duplicates -----------------
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

end;
