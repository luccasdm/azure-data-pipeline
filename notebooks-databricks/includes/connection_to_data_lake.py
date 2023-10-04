# Databricks notebook source
# MAGIC %md
# MAGIC # Estabelecendo conexão

# COMMAND ----------

config = {"fs.azure.account.key.stdataproject01.blob.core.windows.net":dbutils.secrets.get(scope = "key-vault-secret-02", key = "secret-app-id")}

service_credential = dbutils.secrets.get(scope="key-vault-secret-02",key="secret-app-id")
spark.conf.set("fs.azure.account.auth.type.stdataproject01.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.stdataproject01.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.stdataproject01.dfs.core.windows.net", "0908e933-ae29-436e-af3d-4fe766d1432b")
spark.conf.set("fs.azure.account.oauth2.client.secret.stdataproject01.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.stdataproject01.dfs.core.windows.net", "https://login.microsoftonline.com/82734459-337e-47e5-99b3-2c19ee6dc472/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC # Lista de containers

# COMMAND ----------

containers = ["raw-data-2", "transformed-data-2"]

# COMMAND ----------

def mount_datalake(containers):
    try:
        for container in containers:
            dbutils.fs.mount(
                source = f"wasbs://{container}@stdataproject01.blob.core.windows.net",
                mount_point = f"/mnt/{container}",
                extra_configs = config
            )
    except ValueError as err:
        print(err)

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
# MAGIC
# MAGIC ls /mnt
