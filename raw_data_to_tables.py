# Databricks notebook source
# DBTITLE 1,Reading the CSV files and making the delta tables
df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/Volumes/delta_live_tables/default/health_care/diagnosis_mapping.csv")

df.write.format("delta").mode("append").saveAsTable("delta_live_tables.default.diagnosis_mapping")

display(df)

# COMMAND ----------

# DBTITLE 1,Reading the CSV files and creating delta tables
path1 = "/Volumes/delta_live_tables/default/health_care/patients_daily_file_1_2024.csv"
path2 = "/Volumes/delta_live_tables/default/health_care/patients_daily_file_2_2024.csv"
path3 = "/Volumes/delta_live_tables/default/health_care/patients_daily_file_3_2024.csv"

df1 = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{path1}")
print("Before Casting the Admission Date Column to Date Type")
display(df1)
df1 = df1.withColumn("admission_date", df1["admission_date"].cast("date"))
print("After Casting the Admission Date Column to Date Type")
display(df1)

df1.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("delta_live_tables.default.raw_patients_daily")
