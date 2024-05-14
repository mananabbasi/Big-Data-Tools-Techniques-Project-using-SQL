-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("/FileStore/tables/clinicaltrial_2023.zip", "file:/tmp/")

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC ls /tmp/

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC unzip -d /tmp/ /tmp/clinicaltrial_2023.zip

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC ls /tmp/clinicaltrial_2023.csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("file:/tmp/clinicaltrial_2023.csv", "/FileStore/tables/clinicaltrial_2023.csv", True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("FileStore/tables/clinicaltrial_2023.csv/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.head("FileStore/tables/clinicaltrial_2023.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC janirdd = sc.textFile('/FileStore/tables/clinicaltrial_2023.csv')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import Row
-- MAGIC cleanedRDD = janirdd.filter(lambda line: line.strip() != '')
-- MAGIC fieldsRDD = cleanedRDD.map(lambda line: line.split('\t'))
-- MAGIC cleanRDD = fieldsRDD.map(lambda row: [field.rstrip(',') for field in row])
-- MAGIC cleanRDD = cleanRDD.map(lambda fields: [field.replace('"', '') for field in fields])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC cleanedData = cleanRDD.collect()
-- MAGIC for row in cleanedData:
-- MAGIC     print(row)
-- MAGIC def clean_row(row):
-- MAGIC     if len(row) < 14:
-- MAGIC         return row 
-- MAGIC     row = row[:5] + [intervention for field in row[5].split('|') for intervention in field.split(':')] + row[6:]
-- MAGIC     row = row[:7] + [collaborator.strip() for collaborator in row[7].split('|')] + row[8:] if row[7] else row[:7] + [] + row[8:]
-- MAGIC     row = row[:11] + [design for field in row[11].split('|') for design in field.split(':')] + row[12:]
-- MAGIC
-- MAGIC     return row

-- COMMAND ----------

-- MAGIC %python
-- MAGIC header = cleanRDD.first()
-- MAGIC cleanheaderRDD=cleanRDD.filter(lambda row: row != header)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import *
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC # Define the schema for the DataFrame
-- MAGIC schema = StructType([
-- MAGIC     StructField("_1", StringType(), True),
-- MAGIC     StructField("_2", StringType(), True),
-- MAGIC     StructField("_3", StringType(), True),
-- MAGIC     StructField("_4", StringType(), True),
-- MAGIC     StructField("_5", StringType(), True),
-- MAGIC     StructField("_6", StringType(), True),
-- MAGIC     StructField("_7", StringType(), True),
-- MAGIC     StructField("_8", StringType(), True),
-- MAGIC     StructField("_9", StringType(), True),
-- MAGIC     StructField("_10", StringType(), True),
-- MAGIC     StructField("_11", StringType(), True),
-- MAGIC     StructField("_12", StringType(), True),
-- MAGIC     StructField("_13", StringType(), True),
-- MAGIC     StructField("_14", StringType(), True)
-- MAGIC ])
-- MAGIC def create_row(row):
-- MAGIC     if len(row) < 14:
-- MAGIC         return None  # Return None if the row has fewer than 14 fields
-- MAGIC     return Row(*row)
-- MAGIC
-- MAGIC cleandf = cleanRDD.map(create_row).filter(lambda x: x is not None).toDF(schema)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC cleandf= cleandf.withColumnRenamed("_1", "Id").\
-- MAGIC     withColumnRenamed("_2", "Study Title").\
-- MAGIC     withColumnRenamed("_3", "Acronym").\
-- MAGIC     withColumnRenamed("_4", "Status").\
-- MAGIC     withColumnRenamed("_5", "Conditions").\
-- MAGIC     withColumnRenamed("_6", "Interventions").\
-- MAGIC     withColumnRenamed("_7", "Sponsor").\
-- MAGIC     withColumnRenamed("_8", "Collaborators").\
-- MAGIC     withColumnRenamed("_9", "Enrollment").\
-- MAGIC     withColumnRenamed("_10", "Funder Type").\
-- MAGIC     withColumnRenamed("_11", "Type").\
-- MAGIC     withColumnRenamed("_12", "Study Design").\
-- MAGIC     withColumnRenamed("_13","Start").\
-- MAGIC     withColumnRenamed("_14", "Completion")
-- MAGIC
-- MAGIC cleandf.show(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC cleandf.createOrReplaceTempView ("clinicaltrial_2023")

-- COMMAND ----------

select * from clinicaltrial_2023

-- COMMAND ----------

SELECT COUNT(DISTINCT Id) AS distinct_studies FROM clinicaltrial_2023

-- COMMAND ----------

SELECT Type, COUNT(*) AS frequency
FROM clinicaltrial_2023
GROUP BY Type
ORDER BY frequency DESC limit 3;


-- COMMAND ----------


select * from clinicaltrial_2023 limit 3

-- COMMAND ----------

SELECT Conditions FROM clinicaltrial_2023

-- COMMAND ----------

SELECT Condition, COUNT(*) AS Frequency
FROM (
  SELECT explode(split(Conditions, '\\|')) AS Condition
    FROM clinicaltrial_2023)
  GROUP BY Condition
  ORDER BY Frequency DESC
LIMIT 5;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp("/FileStore/tables/pharma.zip", "file:/tmp/")

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC ls /tmp/

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC unzip -d /tmp/ /tmp/pharma.zip

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("file:/tmp/pharma.csv", "/FileStore/tables/pharma.csv", True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("FileStore/tables/pharma.csv/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import Row
-- MAGIC
-- MAGIC janikhanrdd = sc.textFile('/FileStore/tables/pharma.csv/pharma.csv')
-- MAGIC fieldsRDD = janikhanrdd.map(lambda line: line.split(','))
-- MAGIC cleanpharmaRDD = fieldsRDD.map(lambda fields: [field.replace('"', '') for field in fields])
-- MAGIC
-- MAGIC cleanedpharmaData = cleanpharmaRDD.collect()
-- MAGIC for row in cleanedData:
-- MAGIC     print(row)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import Row
-- MAGIC
-- MAGIC schema = StructType([
-- MAGIC     StructField("Company", StringType(), True),
-- MAGIC     StructField("Parent_Company", StringType(), True),
-- MAGIC     StructField("Penalty_Amount", StringType(), True),
-- MAGIC     StructField("Subtraction_From_Penalty", StringType(), True),
-- MAGIC     StructField("Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting", StringType(), True),
-- MAGIC     StructField("Penalty_Year", StringType(), True),
-- MAGIC     StructField("Penalty_Date", StringType(), True),
-- MAGIC     StructField("Offense_Group", StringType(), True),
-- MAGIC     StructField("Primary_Offense", StringType(), True),
-- MAGIC     StructField("Secondary_Offense", StringType(), True),
-- MAGIC     StructField("Description", StringType(), True),
-- MAGIC     StructField("Level_of_Government", StringType(), True),
-- MAGIC     StructField("Action_Type", StringType(), True),
-- MAGIC     StructField("Agency", StringType(), True),
-- MAGIC     StructField("Civil/Criminal", StringType(), True),
-- MAGIC     StructField("Prosecution_Agreement", StringType(), True),
-- MAGIC     StructField("Court", StringType(), True),
-- MAGIC     StructField("Case_ID", StringType(), True),
-- MAGIC     StructField("Private_Litigation_Case_Title", StringType(), True),
-- MAGIC     StructField("Lawsuit_Resolution", StringType(), True),
-- MAGIC     StructField("Facility_State", StringType(), True),
-- MAGIC     StructField("City", StringType(), True),
-- MAGIC     StructField("Address", StringType(), True),
-- MAGIC     StructField("Zip", StringType(), True),
-- MAGIC     StructField("NAICS_Code", StringType(), True),
-- MAGIC     StructField("NAICS_Translation", StringType(), True),
-- MAGIC     StructField("HQ_Country_of_Parent", StringType(), True),
-- MAGIC     StructField("HQ_State_of_Parent", StringType(), True),
-- MAGIC     StructField("Ownership_Structure", StringType(), True),
-- MAGIC     StructField("Parent_Company_Stock_Ticker", StringType(), True),
-- MAGIC     StructField("Major_Industry_of_Parent", StringType(), True),
-- MAGIC     StructField("Specific_Industry_of_Parent", StringType(), True),
-- MAGIC     StructField("Info_Source", StringType(), True),
-- MAGIC     StructField("Notes", StringType(), True)
-- MAGIC ])
-- MAGIC
-- MAGIC
-- MAGIC def create_row(row):
-- MAGIC     if len(row) == 34:
-- MAGIC         return Row(*row)
-- MAGIC     elif len(row) > 34:
-- MAGIC         return Row(*row[:34])
-- MAGIC     else:
-- MAGIC         return None
-- MAGIC
-- MAGIC pharmadf = cleanpharmaRDD.map(create_row).filter(lambda x: x is not None).toDF(schema)
-- MAGIC
-- MAGIC # View the DataFrame
-- MAGIC pharmadf.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharmadf . createOrReplaceTempView ("pharma")

-- COMMAND ----------

SELECT DISTINCT Company AS sponsor 
FROM pharma


-- COMMAND ----------

SELECT c.Sponsor, COUNT(*) AS Trials_Sponsored
FROM clinicaltrial_2023 AS c
LEFT JOIN (
    SELECT DISTINCT Company AS Sponsor 
    FROM pharma
) AS p ON c.Sponsor = p.Sponsor
WHERE p.Sponsor IS NULL  
GROUP BY c.Sponsor
ORDER BY Trials_Sponsored DESC
LIMIT 10;

-- COMMAND ----------

SELECT
    CAST(SUBSTRING(REGEXP_REPLACE(Completion, ',', ''), 6, 2) AS INT) AS Completion_Month,
    COUNT(*) AS Completed_Studies
FROM
    clinicaltrial_2023
WHERE
    Status = 'COMPLETED' AND Completion LIKE '2023%'
GROUP BY
    CAST(SUBSTRING(REGEXP_REPLACE(Completion, ',', ''), 6, 2) AS INT)
ORDER BY
    Completion_Month;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import substring, regexp_replace, col, count, when
-- MAGIC from pyspark.sql.types import IntegerType
-- MAGIC results = spark.sql("""
-- MAGIC     SELECT
-- MAGIC         CAST(SUBSTRING(REGEXP_REPLACE(Completion, ',', ''), 6, 2) AS INT) AS Completion_Month,
-- MAGIC         COUNT(*) AS Completed_Studies
-- MAGIC     FROM
-- MAGIC         clinicaltrial_2023
-- MAGIC     WHERE
-- MAGIC         Status = 'COMPLETED' AND Completion LIKE '2023%'
-- MAGIC     GROUP BY
-- MAGIC         CAST(SUBSTRING(REGEXP_REPLACE(Completion, ',', ''), 6, 2) AS INT)
-- MAGIC     ORDER BY
-- MAGIC         Completion_Month
-- MAGIC """)
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC months = results.select('Completion_Month').rdd.flatMap(lambda x: x).collect()
-- MAGIC completed_studies = results.select('Completed_Studies').rdd.flatMap(lambda x: x).collect()
-- MAGIC plt.figure(figsize=(10, 6))
-- MAGIC plt.bar(months, completed_studies)
-- MAGIC plt.xlabel('Month')
-- MAGIC plt.ylabel('Number of Completed Studies')
-- MAGIC plt.title('Completed Studies by Month in 2023')
-- MAGIC plt.xticks(months)
-- MAGIC plt.show()
-- MAGIC
-- MAGIC

-- COMMAND ----------


