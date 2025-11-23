# Snowflake_ETL_Pipeline


<b></b>
<img width="744" height="121" alt="Untitled Diagram drawio (1)" src="https://github.com/user-attachments/assets/de282799-1272-4009-a67e-bcd5a8985ac2" />

<b><b>

🔹 Architecture

✔ Bronze → Silver → Gold layered architecture

✔ Automated ingestion from S3 using Snowpipe

✔ Stream + Task–based CDC pipeline

✔ Stored procedures for SCD processing

✔ Gold business metrics and semantic calculations

✔ Gold layer was connected with Reporting tools which is used by the business

🔹 Pipeline Architecture


1️⃣ Bronze Layer — Raw Ingestion

Data stored in S3

Configured Storage Integration

Created External Stage

Used Snowpipe to load data automatically into Bronze

Managed duplicates using Bronze Views


2️⃣ Silver Layer — CDC + Transformations

Built Streams on Bronze tables

Used Stored Procedure with MERGE to implement:

SCD-Type logic

Active/Inactive flag

History tracking

Data cleaning (TRIM, COALESCE, INITCAP, etc.)

Automated with Snowflake Tasks, triggered only when new data arrives.


3️⃣ Gold Layer — Business Layer / Semantic Model

Joined Employees + Bonus datasets

Added business rules such as:

Normalized department names

Job level classification

Performance scoring

Total compensation calculation

Used a second task to load/refine data into Gold

Ensured idempotent loads using MERGE (no duplicates!)



🔹 Technologies Used

❄️ Snowflake

🟦 Snowpipe

AWS S3 (Staging Layer)

Streams & Tasks

Stored Procedures (SQL)

Data Modeling (Bronze → Silver → Gold)


🔹 Key Learnings

How to design and automate a real-world ELT pipeline

Stream-based CDC processing

Task-chaining for end-to-end orchestration

Efficient handling of slowly changing dimensions

Semantic modeling for reporting/analytics



