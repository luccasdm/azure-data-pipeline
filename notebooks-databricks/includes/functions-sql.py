# Databricks notebook source
def insertSqlTable(df, table_name, columnTypes='', mode="overwrite", server = 'projects-dev.database.windows.net', system=''):
    
    database_name = "sqldb-projects-dev"
    user = dbutils.secrets.get(scope="key-vault-secret-02",key="secret-sql-user-1")
    password = dbutils.secrets.get(scope="key-vault-secret-02",key="secret-sql-pass-1")

    df.write.mode(mode) \
    .format("jdbc") \
    .option("truncate", "true")\
    .option("url", f"jdbc:sqlserver://{server}:1433;databaseName={database_name};") \
    .option("dbtable", table_name) \
    .option("createTableColumnTypes",columnTypes)\
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

# COMMAND ----------

def readSqlTable(table_name, server = 'projects-dev.database.windows.net', database = 'sqldb-projects-dev'):
    

    database_name = "sqldb-projects-dev"
    user = dbutils.secrets.get(scope="key-vault-secret-02",key="secret-sql-user-1")
    password = dbutils.secrets.get(scope="key-vault-secret-02",key="secret-sql-pass-1")

    jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver://{server}:1433;databaseName={database_name};") \
    .option("dbtable", table_name) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()
    
    return jdbcDF
