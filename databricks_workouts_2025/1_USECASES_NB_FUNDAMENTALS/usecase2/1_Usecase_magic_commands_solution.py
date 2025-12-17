# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #**Inceptez Technologies team meet up *
# MAGIC
# MAGIC ## *Task Overview*
# MAGIC This notebook demonstrates how to document your work using Markdown in Databricks.  
# MAGIC You will learn to use headings, bold, italics, colored text, and images.
# MAGIC
# MAGIC ---
# MAGIC ## **created by **
# MAGIC <span style="color:teal"><b>Nujuma</b></span>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Team Photo**
# MAGIC <img src="https://fpimages.withfloats.com/actual/6929d1ac956d0a744b5c9822.jpeg" alt="Team Photo" width="400"/>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS  usage_metrics 

# COMMAND ----------

import requests 
response=requests.get("https://public.tableau.com/app/sample-data/mobile_os_usage.csv") 
dbutils.fs.put ("/Volumes/workspace/default/usage_metrics/mobile_os_usage.csv" ,response.text,overwrite=True)

# COMMAND ----------

# MAGIC %run
# MAGIC "/Workspace/Users/nujumasyed@gmail.com/databricks-code-repo/databricks_workouts_2025/1_USECASES_NB_FUNDAMENTALS/4_child_nb_dataload"
# MAGIC
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls ("/Volumes/workspace/default/usage_metrics"))
print(dbutils.fs.head ("/Volumes/workspace/default/usage_metrics/mobile_os_usage.csv"))


# COMMAND ----------

# List files in the volume to check if the file is created
files=dbutils.fs.ls("/Volumes/workspace/default/usage_metrics")
display(files)
# Display the first few lines of the file using %fs head
# Note: %fs is a magic command and should be run in a notebook cell directly, not inside Python code.
# You can use the following in a notebook cell:
files_head =dbutils.fs.head("/Volumes/workspace/default/usage_metrics/mobile_os_usage.csv",1000)
print(files_head)

# COMMAND ----------

#Task6: Create a pyspark dataframe df1 reading the data from the above file using pyspark magic command %python

df1=spark.read.csv("/Volumes/workspace/default/usage_metrics/mobile_os_usage.csv",header=True,inferSchema=True)
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC DROP TABLE IF EXISTS default.mobile_os_usage

# COMMAND ----------

df1=spark.read.csv("/Volumes/workspace/default/usage_metrics/mobile_os_usage_1.csv",header=False ,inferSchema=True)
display(df1)
df2=df1.toDF("Date","Mobile_operating_system","Percent_of_usage")
display(df2)
table_name="workspace.default.mobile_os_usage"
df2.write.mode("overwrite").saveAsTable(table_name)
print("Data has been written to the table ")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC

# COMMAND ----------

#Task8: Write sql query to display the data loaded into the table 'default.mobile_os_usage' using the pyspark magic command %python
sql ="select * from default.mobile_os_usage"
display(spark.sql(sql))


# COMMAND ----------

#Task9: Create a python function to convert the given input to upper case

def convert_uppercase(input):
  return input.upper()
convert_uppercase("nujuma")
#Task10: Create a python function to convert the given input to lower case


# COMMAND ----------

#Task10: Install pandas library using the pip python magic command %pip
%pip install pandas

# COMMAND ----------

#Task11: Import pandas, using pandas read_csv and display the output using the magic command %python
import pandas as pd
df = pd.read_csv("/Volumes/workspace/default/usage_metrics/mobile_os_usage.csv")
display(df)

# COMMAND ----------

# MAGIC %%sh
# MAGIC echo "Magic commands tasks completed"
