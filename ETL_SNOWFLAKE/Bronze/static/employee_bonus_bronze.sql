-- This is the manually maintained table
-- Lets assume this is a Static table

CREATE OR REPLACE TABLE etl_snowflake.etl_bronze.employee_bonus_bronze (
    employee_id INT,                
    bonus_year INT,
    annual_bonus NUMBER(10,2),
    performance_score NUMBER(3,1),  
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
