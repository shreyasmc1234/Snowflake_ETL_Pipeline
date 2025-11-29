------------------- Warehouse Creation --------------------------

CREATE OR REPLACE WAREHOUSE etl_wh
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;


-------------- Storage Integration ------------------------------


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

------------- Pipe to automatically Ingest the data --------------

CREATE OR REPLACE PIPE s3_to_bronze
AUTO_INGEST=TRUE
AS
COPY INTO etl_snowflake.etl_bronze.employee_bronze
FROM @external_stage_s3_bronze;

--------- Task will trigger if there is a new/changed data in bronze--------------------------

create or replace task bronze_to_silver
warehouse='etl_wh'
-- schedule='USING CRON 0 */10 * * * Asia/Kolkata'
when system$stream_has_data('etl_snowflake.etl_bronze.stream1')
as
CALL etl_snowflake.etl_silver.employee_silver_history();

------------Below task will trigger after the first task ------------------------------------

create or replace task silver_to_gold
warehouse='etl_wh'
after bronze_to_silver
as
CALL  etl_snowflake.etl_gold.employee_gold_load();


------------To Resume and Pause the Pipe and Tasks --------------------------------------------

Alter pipe my_pipe pipe_execution_paused=TRUE;
Alter pipe mypipe resume; 
Alter pipe mypipe pause; 

Alter task bronze_to_silver pause; 
Alter task bronze_to_silver resume; 

Alter task silver_to_gold pause; 
Alter task silver_to_gold resume;
-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------









