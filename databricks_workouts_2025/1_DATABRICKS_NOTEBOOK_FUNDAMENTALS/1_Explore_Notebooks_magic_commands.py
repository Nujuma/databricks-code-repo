# Databricks notebook source
# MAGIC %md
# MAGIC #Welcome to Inceptez Technologies 
# MAGIC Let us understand about creating notebooks & magical commands
# MAGIC https://fplogoimages.withfloats.com/actual/68009c3a43430aff8a30419d.png
# MAGIC ![](https://fplogoimages.withfloats.com/actual/68009c3a43430aff8a30419d.png)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Let us learn first about Magical Commands
# MAGIC **Important Magic Commands**
# MAGIC - %run: runs a Python file or a notebook.
# MAGIC - %sh: executes shell commands on the cluster nodes.
# MAGIC - %fs: allows you to interact with the Databricks file system.
# MAGIC - %sql: allows you to run Spark SQL/HQL queries.
# MAGIC - %python: switches the notebook context to Python.
# MAGIC - %md: allows you to write markdown text.
# MAGIC - %pip: allows you to install Python packages.
# MAGIC
# MAGIC **Not Important Magic Commands or We learn few of these where we have Cloud(Azure) dependency**
# MAGIC - %scala: switches the notebook context to Scala.
# MAGIC - %r: switches the notebook context to R.
# MAGIC - %lsmagic: lists all the available magic commands.
# MAGIC - %config: allows you to set configuration options for the notebook.
# MAGIC - %load: loads the contents of a file into a cell.
# MAGIC - %who: lists all the variables in the current scope.

# COMMAND ----------

# MAGIC %md
# MAGIC ####How to call a notebook from the current notebook using %run magic command

# COMMAND ----------

# MAGIC %run
# MAGIC "/Workspace/Users/nujumasyed@gmail.com/databricks-code-repo/databricks_workouts_2025/1_DATABRICKS_NOTEBOOK_FUNDAMENTALS/my_first_ntbk"

# COMMAND ----------

# MAGIC %md
# MAGIC ####How to run a linux commands inside a notebook using %sh magic command

# COMMAND ----------

# MAGIC %sh
# MAGIC ls  /databricks-datasets
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.volume1;

# COMMAND ----------

# MAGIC %md
# MAGIC ####How to run a DBFS (Hadoop) FS commands inside a notebook using %fs magic command <br> upload some sample data into the created volume

# COMMAND ----------

dbutils.fs.cp(
    "/Volumes/workspace/default/volume1/accounts.csv","/Volumes/workspace/default/volume1/accounts1.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ####How to run a Spark SQL/HQL Queries inside a notebook using %sql magic command

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists cities(id int,city string);
# MAGIC insert into cities values(3,'Mumbai'),(4,'Lucknow');

# COMMAND ----------

# MAGIC %sql
# MAGIC update cities set city='Kolkata' where id=4;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cities;
# MAGIC from cities select *;

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table default.cities;

# COMMAND ----------

# MAGIC %md
# MAGIC ####How to run a Python Program inside a notebook using %python  magic command or by default the cell will be enabled with python interpretter only

# COMMAND ----------

def sqrt(a):
    return a*a

# COMMAND ----------

print("square root function call ",sqrt(10))

# COMMAND ----------

df1=spark.read.csv("/Volumes/workspace/default/volume1/accounts.csv")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ######How to install additional libraries in this current Python Interpreter using %pip magic command

# COMMAND ----------

# MAGIC %pip install pypi
