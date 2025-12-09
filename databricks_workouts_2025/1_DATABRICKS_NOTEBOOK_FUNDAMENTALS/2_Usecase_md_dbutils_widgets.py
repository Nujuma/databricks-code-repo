# Databricks notebook source
# MAGIC %md
# MAGIC ![](https://fplogoimages.withfloats.com/actual/68009c3a43430aff8a30419d.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Healthcare Data Utilities Usecase2
# MAGIC
# MAGIC ## Objective
# MAGIC This notebook demonstrates how to design Databricks notebooks using Markdown
# MAGIC and how to work with Databricks utilities such as dbutils.fs, dbutils.widgets,
# MAGIC and dbutils.notebook using Volumes.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project Workflow
# MAGIC 1. Create folder structure using Volumes
# MAGIC 2. Create sample healthcare data
# MAGIC 3. Perform file operations using dbutils.fs
# MAGIC 4. Parameterize execution using widgets
# MAGIC 5. Exit notebook with execution status

# COMMAND ----------

# MAGIC %md
# MAGIC ## Folder Structure
# MAGIC
# MAGIC | Folder | Purpose |
# MAGIC |------|---------|
# MAGIC | raw | Incoming healthcare files |
# MAGIC | processed | Validated healthcare data |
# MAGIC | archive | Historical data |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Outcome
# MAGIC Our Aspirants will understand notebook design, parameterization, and fs, notebook, widgets using Databricks utilities.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Define Base Paths using python variable <br>
# MAGIC base_path = "/Volumes/workspace/default/volumewd36" <br>
# MAGIC Create raw_path, processed_path and archive_path as given below... <br>
# MAGIC raw_path = f"{base_path}/raw" <br>
# MAGIC processed_path = f"{base_path}/processed" <br>
# MAGIC archive_path = f"{base_path}/archive"

# COMMAND ----------

base_path = "/Volumes/workspace/default/volumewd36"
#Create raw_path, processed_path and archive_path as given below...
raw_path = f"{base_path}/raw"
print(raw_path)
processed_path = f"{base_path}/processed"
print(processed_path)
archive_path = f"{base_path}/archive"
print(archive_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. dbutils Usecase – Create Directories using the above path variables..

# COMMAND ----------


dbutils.fs.mkdirs(raw_path)
dbutils.fs.mkdirs(processed_path)
dbutils.fs.mkdirs(archive_path)
print(f" Directoy has been created sucessfully ......{raw_path},{processed_path},{archive_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC 3. dbutils Usecase – Create Sample Healthcare File <br>
# MAGIC sample_data = """patient_id,patient_name,age,gender
# MAGIC 1,John Doe,68,M
# MAGIC 2,Jane Smith,54,F
# MAGIC """
# MAGIC
# MAGIC TODO: Write this file content into raw folder created earlier... using dbutils.fs.......

# COMMAND ----------

sample_data = """patient_id,patient_name,age,gender 1,John Doe,68,M 2,Jane Smith,54,F """
base_path = "/Volumes/workspace/default/volumewd36"
#Create raw_path, processed_path and archive_path as given below...
raw_path = f"{base_path}/raw"
print(raw_path)
dbutils.fs.put(f"{raw_path}/sample_data.txt", sample_data ,overwrite=True)
print(f" File has been created sucessfully ......{raw_path}/sample_data.txt")


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 4. dbutils Usecase - list the file created <br>
# MAGIC TODO: List all files available in raw folder using the dbutils command <br>
# MAGIC dbutils.fs......

# COMMAND ----------

dbutils.fs.ls(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 5. dbutils Usecase – Copy File (raw → processed)

# COMMAND ----------

dbutils.fs.cp(raw_path, processed_path, True)
dbutils.fs.ls(processed_path)
print(" File has been copied sucessfully")

# COMMAND ----------

# MAGIC %md
# MAGIC 6. dbutils widget usecase - Create dropdown and text widgets... <br>
# MAGIC TODO: Create a dropdown widget for environment (dev, qa, prod) using <br>
# MAGIC TODO: Create a text widget for owner name
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.widgets.dropdown("environment","dev",["dev","test","prod"])
dbutils.widgets.text("Owner Name","Nujuma")
print(dbutils.widgets.get("environment"))
print(dbutils.widgets.get("Owner Name"))

# COMMAND ----------

# MAGIC %md
# MAGIC 7. dbutils widget Usecase – Read Widget Values environment and owner and print in the screen

# COMMAND ----------

print (f"environment : {dbutils.widgets.get('environment')}")
print (f"Owner Name : {dbutils.widgets.get('Owner Name')}")  
       

# COMMAND ----------

# MAGIC %md
# MAGIC 8. dbutils widget Usecase – Move the above processed File to Archive

# COMMAND ----------

dbutils.fs.mv(processed_path,archive_path,True)
dbutils.fs.ls(archive_path)
print(" File has been moved sucessfully")   

# COMMAND ----------

# MAGIC %md
# MAGIC 9. dbutils notebook usecase - Run the notebook4 using the dbutils command
# MAGIC /Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/databricks_workouts_2025/1_USECASES_NB_FUNDAMENTALS/4_child_nb_dataload

# COMMAND ----------

dbutils.notebook.run ("/Workspace/Users/nujumasyed@gmail.com/databricks-code-repo/databricks_workouts_2025/1_USECASES_NB_FUNDAMENTALS/4_child_nb_dataload",100)


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 10. dbutils notebook usecase - exit this notebook 
# MAGIC TODO: Exit notebook with a success message
# MAGIC dbutils.notebook._____("Pipeline completed successfully")
# MAGIC

# COMMAND ----------

dbutils.notebook.exit ("Pipeline Completed sucessfully")
