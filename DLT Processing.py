# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE LIVE TABLE mapping
# MAGIC COMMENT "Bronze Table for mapping file"
# MAGIC TBLPROPERTIES ("quality" = "bronze")
# MAGIC AS
# MAGIC SELECT * FROM delta_live_tables.default.diagnosis_mapping
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE daily_patients
# MAGIC COMMENT "Bronze Table for Daily Patients"
# MAGIC TBLPROPERTIES ("quality" = "bronze")
# MAGIC AS 
# MAGIC SELECT * FROM STREAM(delta_live_tables.default.raw_patients_daily)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE processed_data
# MAGIC COMMENT "Silver Table for Processed Data"
# MAGIC TBLPROPERTIES ("quality" = "silver")
# MAGIC AS 
# MAGIC SELECT 
# MAGIC     p.patient_id,
# MAGIC     p.name,
# MAGIC     p.age,
# MAGIC     p.gender,
# MAGIC     p.address,
# MAGIC     p.contact_number,
# MAGIC     p.admission_date,
# MAGIC     m.diagnosis_description
# MAGIC FROM STREAM(live.daily_patients) p LEFT JOIN live.mapping m ON p.diagnosis_code = m.diagnosis_code
# MAGIC
# MAGIC
# MAGIC -- For Validation we have to make sure that product Edition Must be Advanced while creating a job in Delta Live Table under Data Engineering Section
# MAGIC
# MAGIC -- CREATE OR REFRESH STREAMING TABLE processed_patient_data(CONSTRAINT valid_data EXPECT (patient_id IS NOT NULL and `name` IS NOT NULL and age IS NOT NULL and gender IS NOT NULL and `address` IS NOT NULL and contact_number IS NOT NULL and admission_date IS NOT NULL) ON VIOLATION DROP ROW)
# MAGIC -- COMMENT "Silver table with newly joined data from bronze tables and data quality constraints"
# MAGIC -- TBLPROPERTIES ("quality" = "silver")
# MAGIC -- AS
# MAGIC -- SELECT
# MAGIC --     p.patient_id,
# MAGIC --     p.name,
# MAGIC --     p.age,
# MAGIC --     p.gender,
# MAGIC --     p.address,
# MAGIC --     p.contact_number,
# MAGIC --     p.admission_date,
# MAGIC --     m.diagnosis_description
# MAGIC -- FROM STREAM(live.daily_patients) p
# MAGIC -- LEFT JOIN live.diagnostic_mapping m
# MAGIC -- ON p.diagnosis_code = m.diagnosis_code;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE LIVE TABLE patient_statistic_by_diagnosis
# MAGIC COMMENT "Gold Table with detailed patient statistics by diagnosis"
# MAGIC TBLPROPERTIES ("quality" = "gold")
# MAGIC AS
# MAGIC SELECT 
# MAGIC
# MAGIC     diagnosis_description,
# MAGIC     COUNT(patient_id) AS patient_count,
# MAGIC     AVG(age) AS avg_age,
# MAGIC     COUNT(DISTINCT gender) AS unique_gender_count,
# MAGIC     MIN(age) AS min_age,
# MAGIC     MAX(age) AS max_age
# MAGIC
# MAGIC FROM live.processed_data
# MAGIC GROUP BY diagnosis_description;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE LIVE TABLE patient_statistics_by_gender
# MAGIC COMMENT "Gold table with detailed patient statistics by gender"
# MAGIC TBLPROPERTIES ("quality" = "gold")
# MAGIC AS
# MAGIC SELECT
# MAGIC     gender,
# MAGIC     COUNT(patient_id) AS patient_count,
# MAGIC     AVG(age) AS avg_age,
# MAGIC     COUNT(DISTINCT diagnosis_description) AS unique_diagnosis_count,
# MAGIC     MIN(age) AS min_age,
# MAGIC     MAX(age) AS max_age
# MAGIC FROM live.processed_data
# MAGIC GROUP BY gender;
