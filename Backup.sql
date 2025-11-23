CREATE OR REPLACE WAREHOUSE etl_wh
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

create or replace database etl_snowflake;

USE DATABASE etl_snowflake;

create or replace schema etl_bronze;
create or replace schema etl_silver;
create or replace schema etl_gold;



--Storage Integration

CREATE OR REPLACE STORAGE INTEGRATION my_etl_integration
TYPE=EXTERNAL_STAGE
ENABLED=TRUE
STORAGE_PROVIDER='S3'
STORAGE_ALLOWED_LOCATIONS=('s3://your-bucket/path/')
STORAGE_AWS_ROLE_ARN='arn:aws:iam::<your-account-id>:role/<your-s3-role>';

CREATE OR REPLACE STAGE external_stage_s3_bronze
file_format=(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
pattern='.*csv.*'
storage_integration=my_etl_integration
url='s3://your-bucket/path/';

CREATE OR REPLACE PIPE s3_to_bronze
AUTO_INGEST=TRUE
AS
COPY INTO etl_snowflake.etl_bronze.employee_bronze
FROM @external_stage_s3_bronze;


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

drop table etl_snowflake.etl_bronze.employee_bronze;

INSERT INTO etl_snowflake.etl_bronze.employee_bronze (employee_id, first_name, last_name, email, department, hire_date, salary, source_file)
VALUES
(1, 'John', 'Doe', 'john.doe@example.com', 'HR', '2020-01-15', 55000, 'manual_load.csv'),
(2, 'Jane', 'Smith', 'jane.smith@example.com', 'Finance', '2019-03-22', 65000, 'manual_load.csv'),
(3, 'Alice', 'Johnson', 'alice.johnson@example.com', 'IT', '2021-07-10', 72000, 'manual_load.csv'),
(4, 'Bob', 'Brown', 'bob.brown@example.com', 'Marketing', '2018-11-05', 58000, 'manual_load.csv'),
(5, 'Carol', 'Davis', 'carol.davis@example.com', 'Sales', '2020-06-18', 60000, 'manual_load.csv'),
(6, 'David', 'Wilson', 'david.wilson@example.com', 'IT', '2019-09-25', 71000, 'manual_load.csv'),
(7, 'Eva', 'Taylor', 'eva.taylor@example.com', 'Finance', '2021-02-14', 64000, 'manual_load.csv'),
(8, 'Frank', 'Anderson', 'frank.anderson@example.com', 'Marketing', '2020-12-30', 59000, 'manual_load.csv'),
(9, 'Grace', 'Thomas', 'grace.thomas@example.com', 'HR', '2019-08-09', 56000, 'manual_load.csv'),
(10, 'Henry', 'Moore', 'henry.moore@example.com', 'Sales', '2018-05-21', 62000, 'manual_load.csv'),
(11, 'Ivy', 'Martin', 'ivy.martin@example.com', 'IT', '2021-04-12', 73000, 'manual_load.csv'),
(12, 'Jack', 'Lee', 'jack.lee@example.com', 'Finance', '2020-07-08', 66000, 'manual_load.csv'),
(13, 'Kathy', 'Perez', 'kathy.perez@example.com', 'Marketing', '2019-10-16', 60000, 'manual_load.csv'),
(14, 'Leo', 'White', 'leo.white@example.com', 'HR', '2021-01-25', 57000, 'manual_load.csv'),
(15, 'Mona', 'Harris', 'mona.harris@example.com', 'Sales', '2020-03-03', 63000, 'manual_load.csv'),
(16, 'Nick', 'Clark', 'nick.clark@example.com', 'IT', '2019-12-19', 72000, 'manual_load.csv'),
(17, 'Olivia', 'Lewis', 'olivia.lewis@example.com', 'Finance', '2021-06-21', 65000, 'manual_load.csv'),
(18, 'Paul', 'Robinson', 'paul.robinson@example.com', 'Marketing', '2020-09-14', 61000, 'manual_load.csv'),
(19, 'Quinn', 'Walker', 'quinn.walker@example.com', 'HR', '2019-02-28', 55000, 'manual_load.csv'),
(20, 'Rachel', 'Hall', 'rachel.hall@example.com', 'Sales', '2021-08-17', 64000, 'manual_load.csv');

delete from etl_snowflake.etl_bronze.employee where employee_id=20;

update etl_snowflake.etl_bronze.employee_bronze set first_name='Racheal' where employee_id=20;
--- Streams to capture the changes and loading the data into silver layer

create or replace stream etl_snowflake.etl_bronze.stream1 on table etl_snowflake.etl_bronze.employee_bronze;
create or replace stream etl_snowflake.etl_bronze.stream2 on table etl_snowflake.etl_bronze.employee_bronze;

select * from etl_snowflake.etl_bronze.stream1;
select * from etl_snowflake.etl_bronze.stream2;


-- Creating table in silver layer loading the data to silver by implementing SCD
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
end;




CALL etl_snowflake.etl_silver.employee_silver_history();


---------------------------------------------------------------------
-- Assume this is a static table

CREATE OR REPLACE TABLE etl_snowflake.etl_bronze.employee_bonus_bronze (
    employee_id INT,                -- foreign key to employee_bronze
    bonus_year INT,
    annual_bonus NUMBER(10,2),
    performance_score NUMBER(3,1),  -- optional metric
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


INSERT INTO etl_snowflake.etl_bronze.employee_bonus_bronze (employee_id, bonus_year, annual_bonus, performance_score)
VALUES
(1, 2023, 5000, 4.5),
(2, 2023, 6000, 4.7),
(3, 2023, 7200, 4.9),
(4, 2023, 5800, 4.2),
(5, 2023, 6100, 4.6),
(6, 2023, 7100, 4.8),
(7, 2023, 6400, 4.4),
(8, 2023, 5900, 4.3),
(9, 2023, 5600, 4.5),
(10, 2023, 6200, 4.6),
(11, 2023, 7300, 4.9),
(12, 2023, 6600, 4.7),
(13, 2023, 6000, 4.2),
(14, 2023, 5700, 4.3),
(15, 2023, 6300, 4.5),
(16, 2023, 7200, 4.8),
(17, 2023, 6500, 4.6),
(18, 2023, 6100, 4.4),
(19, 2023, 5500, 4.1),
(20, 2023, 6400, 4.5);

INSERT INTO etl_snowflake.etl_bronze.employee_bonus_bronze (employee_id, bonus_year, annual_bonus, performance_score)
VALUES (1, 2023, 5000, 4.5),
(2, 2023, 6000, 4.7),
(3, 2023, 7200, 4.9);


delete from etl_snowflake.etl_bronze.employee_bonus_bronze where employee_id in (14,15,17,18,3,4,6,7,10,20);

--Implementing Deduplication logic
create or replace view etl_snowflake.etl_bronze.employee_bonus_bronze_view 
as
select 
employee_id,
bonus_year,
annual_bonus, 
performance_score from
etl_snowflake.etl_bronze.employee_bonus_bronze 
qualify row_number() over(partition by employee_id order by employee_id asc) =1
order by employee_id;


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

select * from ETL_SNOWFLAKE.ETL_SILVER.EMPLOYEE_SILVER_view ;
select * from etl_snowflake.etl_bronze.employee_bonus_bronze_view ;

INSERT INTO etl_snowflake.etl_gold.employee_gold
select 
a.employee_id as id,
initcap(a.full_name) as name,
a.email as email_address,
coalesce((salary+a.annual_bonus),0) as total_salary,
case when department in ('HR','Hr','hr','hR') then 'Human Resource'
     when department in ('IT') then 'Information Technology'
     ELSE DEPARTMENT end as Deparment_name,
case when months_between(current_date(),a.hire_date)>72 then 'Senior Consulant'
     when months_between(current_date(),a.hire_date)<=72 and months_between(current_date(),a.hire_date)>48 then 'Consultant'
     else 'Analyst'
end as Job_Level,
Case when performance_score>=5 then 'High Performer'
     when performance_score<5 and performance_score>3 then 'Average Performer'
     else 'Low Performer' 
end as Performance_status
from ETL_SNOWFLAKE.ETL_SILVER.EMPLOYEE_SILVER_view a
left join etl_snowflake.etl_silver.employee_bonus_silver b
on a.employee_id=b.employee_id;

CREATE OR REPLACE TABLE etl_snowflake.etl_gold.employee_gold (
    id NUMBER(38,0),                       
    name VARCHAR(255),                       
    email_address VARCHAR(255),             
    total_salary NUMBER(10,2),              
    department_name VARCHAR(100),            
    job_level VARCHAR(50),                   
    performance_status VARCHAR(50),          
    load_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() 
);

create or replace procedure etl_snowflake.etl_gold.employee_gold_load()
returns string
language sql
as
begin

Merge into etl_snowflake.etl_gold.employee_gold target
using (
select 
a.employee_id as id,
initcap(a.full_name) as name,
a.email as email_address,
coalesce((salary+a.annual_bonus),0) as total_salary,
case when department in ('HR','Hr','hr','hR') then 'Human Resource'
     when department in ('IT') then 'Information Technology'
     ELSE DEPARTMENT end as Department_name,
case when months_between(current_date(),a.hire_date)>72 then 'Senior Consulant'
     when months_between(current_date(),a.hire_date)<=72 and months_between(current_date(),a.hire_date)>48 then 'Consultant'
     else 'Analyst'
end as Job_Level,
Case when performance_score>=5 then 'High Performer'
     when performance_score<5 and performance_score>3 then 'Average Performer'
     else 'Low Performer' 
end as Performance_status
from ETL_SNOWFLAKE.ETL_SILVER.EMPLOYEE_SILVER_view a
left join etl_snowflake.etl_silver.employee_bonus_silver b
on a.employee_id=b.employee_id
) source

on target.id=source.id
when matched then 
UPDATE SET
        target.name = source.name,
        target.email_address = source.email_address,
        target.total_salary = source.total_salary,
        target.Department_name = source.Department_name,
        target.Job_Level = source.Job_Level,
        target.Performance_status = source.Performance_status,
        target.load_time=current_timestamp()
when not matched then 
insert (id,name,email_address,total_salary,Department_name,job_level,performance_status,load_time)
values(source.id,source.name,source.email_address,source.total_salary,source.Department_name,source.job_level,source.performance_status,current_timestamp());

return  'Loaded to gold layer';
end;



CALL  etl_snowflake.etl_gold.employee_gold_load();


create or replace task bronze_to_silver
warehouse='etl_wh'
-- schedule='USING CRON 0 */10 * * * Asia/Kolkata'
when system$stream_has_data('etl_snowflake.etl_bronze.stream1')
as
CALL etl_snowflake.etl_silver.employee_silver_history();


create or replace task silver_to_gold
warehouse='etl_wh'
after bronze_to_silver
as
CALL  etl_snowflake.etl_gold.employee_gold_load();
