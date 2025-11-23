use schema etl_snowflake.etl_gold;

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
