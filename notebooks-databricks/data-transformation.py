# Databricks notebook source
user = dbutils.secrets.get(scope="key-vault-secret-02", key="secret-sql-user-teste")

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "0908e933-ae29-436e-af3d-4fe766d1432b",
"fs.azure.account.oauth2.client.secret": 'hao8Q~HTG0VqO8d3AuVRi4gtKl-parvYVYSVmafv',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/82734459-337e-47e5-99b3-2c19ee6dc472/oauth2/token"}


dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@stdataproject01.dfs.core.windows.net", 
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymic/raw-data"

# COMMAND ----------

# MAGIC %md
# MAGIC # Lendo os arquivos

# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/medals.csv")
teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/teams.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC # Armazenando na camada Transformed

# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transformed-data/athletes")
coaches.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/coaches")
entriesgender.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/entriesgender")
medals.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/medals")
teams.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/teams")

# COMMAND ----------

# Calculate the average number of entries by gender for each discipline
average_entries_by_gender = entriesgender.withColumn(
                                                'Avg_Female', entriesgender['Female'] / entriesgender['Total']
                                            ).withColumn(
                                                'Avg_Male', entriesgender['Male'] / entriesgender['Total']
                                            )
average_entries_by_gender.show()

# COMMAND ----------

average_entries_by_gender.write.mode("overwrite").option("header",'true').parquet("/mnt/tokyoolymic/transformed-data/average_entries_by_gender")
