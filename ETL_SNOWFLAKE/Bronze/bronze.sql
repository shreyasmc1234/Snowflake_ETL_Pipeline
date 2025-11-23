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


create or replace stream etl_snowflake.etl_bronze.stream1 on table etl_snowflake.etl_bronze.employee_bronze;
create or replace stream etl_snowflake.etl_bronze.stream2 on table etl_snowflake.etl_bronze.employee_bronze;

select * from etl_snowflake.etl_bronze.stream1;
select * from etl_snowflake.etl_bronze.stream2;
