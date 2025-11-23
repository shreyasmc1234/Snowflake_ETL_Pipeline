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
