-- Databricks notebook source
-- MAGIC %python
-- MAGIC 
-- MAGIC #Create a variable to take any uploaded file (clinicaltrial files) to ensure code automation
-- MAGIC 
-- MAGIC fileroot = "clinicaltrial_2021"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC #Split the pharma file into lines by the delimiter("|")
-- MAGIC 
-- MAGIC clinical = spark.read.options(delimiter ="|", header= True).csv("/FileStore/tables/" + fileroot)
-- MAGIC clinical.show(5, truncate = False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinical . createOrReplaceTempView ("clinical")

-- COMMAND ----------

SELECT * FROM clinical

-- COMMAND ----------

--Create a permanent table for clinical dataset
CREATE OR REPLACE TABLE default.clinical_perm AS SELECT * FROM clinical

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- DBTITLE 1,Question 1
SELECT distinct(count(*)) AS Total_Study FROM clinical 

-- COMMAND ----------

-- DBTITLE 1,Question 2
SELECT Type, count(Type) AS Frequency FROM clinical
GROUP BY Type
ORDER BY Frequency desc

-- COMMAND ----------

-- DBTITLE 1,Question 3
--To split all rows in the condition column by the delimiter(,), group the resulted table by condition column, count each conditions frequency and order by frequency

SELECT condition, COUNT(*) AS Frequency
FROM (
    SELECT explode(split(Conditions, ',')) AS condition
    FROM clinical
    WHERE Conditions IS NOT NULL
)
GROUP BY condition
ORDER BY Frequency DESC
LIMIT 5;

-- COMMAND ----------

CREATE TEMPORARY VIEW pharma
USING csv
OPTIONS (path "/FileStore/tables/pharma/pharma.csv", header "true", inferSchema "true")

-- COMMAND ----------

-- DBTITLE 1,Question 4
SELECT clinical.Sponsor, COUNT(*) AS Count_of_Sponsor
FROM clinical
LEFT JOIN pharma ON clinical.Sponsor = pharma.Parent_Company
WHERE pharma.Parent_Company IS NULL AND clinical.Sponsor IS NOT NULL
GROUP BY clinical.Sponsor
ORDER BY Count_of_Sponsor DESC
LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,Question 5
-- Select the columns Status and Completion from the table clinical
SELECT Status, Completion
FROM clinical;

-- Filter the table to only include rows where the Completion column contains the year 2019 and the Status column is 'Completed', then group by the Completion column and count the number of occurrences
SELECT Completion, COUNT(*) AS count
FROM clinical
WHERE Completion LIKE '%2021%' AND Status = 'Completed'
GROUP BY Completion;


-- COMMAND ----------

-- Add a new column called month that extracts the first three characters from the Completion column, then select only the month and count columns
SELECT SUBSTRING(Completion, 1, 3) AS month, COUNT(*) AS count
FROM clinical
WHERE Completion LIKE '%2021%' AND Status = 'Completed'
GROUP BY month;

-- COMMAND ----------

-- Add a new column called number that maps each month abbreviation to a corresponding number, then display the resulting table

CREATE OR REPLACE TABLE default.monthly_complete AS SELECT *
FROM(

SELECT 
  month,
  count,
  CASE month
    WHEN 'Jan' THEN 'a'
    WHEN 'Feb' THEN 'b'
    WHEN 'Mar' THEN 'c'
    WHEN 'Apr' THEN 'd'
    WHEN 'May' THEN 'e'
    WHEN 'Jun' THEN 'f'
    WHEN 'Jul' THEN 'g'
    WHEN 'Aug' THEN 'h'
    WHEN 'Sep' THEN 'i'
    WHEN 'Oct' THEN 'j'
    WHEN 'Nov' THEN 'k'
    ELSE 'l'
  END AS number
FROM (
  SELECT SUBSTRING(Completion, 1, 3) AS month, COUNT(*) AS count
  FROM clinical
  WHERE Completion LIKE '%2021%' AND Status = 'Completed'
  GROUP BY month
) t
ORDER BY number);

-- COMMAND ----------

SELECT * FROM default.monthly_complete

-- COMMAND ----------



-- COMMAND ----------

-- DBTITLE 1,Extra Question - What is the frequency of trials by level_of_gov in the United kingdom that are publicly traded?
SELECT *
FROM pharma;


-- COMMAND ----------

SELECT *
FROM clinical
JOIN pharma ON clinical.Sponsor = pharma.Parent_Company

-- COMMAND ----------

SELECT p.Level_of_Government, count(c.Id) AS Frequency
FROM clinical c
JOIN pharma p ON c.Sponsor = p.Parent_Company
WHERE p.HQ_Country_of_Parent = 'United Kingdom' AND p.Ownership_Structure = 'publicly traded'
GROUP BY p.Level_of_Government
ORDER BY count(c.Id) desc

-- COMMAND ----------


