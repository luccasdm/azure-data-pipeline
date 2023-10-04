# Databricks notebook source
# MAGIC %run ../notebooks-databricks/includes/functions-sql

# COMMAND ----------

athletes_transformed = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/athletes.csv")
coaches_transformed = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/coaches.csv")
entriesgender_transformed = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/entriesgender.csv")
medals_transformed = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/medals.csv")
teams_transformed = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/teams.csv")

# COMMAND ----------

columns_athletes = 'PersonName varchar(50),\
                    Country varchar(50),\
                    Discipline varchar(50)'

columns_coaches = 'Name varchar(50),\
                Country varchar(50),\
                Discipline varchar(50),\
                Event varchar(50)'

columns_entriesgender = 'Discipline varchar(50),\
                            Female varchar(50),\
                            Male varchar(50),\
                            Total varchar(50)'

columns_medals = 'Rank varchar(50),\
                Team_Country varchar(50),\
                Gold varchar(50),\
                Silver varchar(50),\
                Bronze varchar(50),\
                Total varchar(50)'

columns_teams = 'TeamName varchar(50),\
                Discipline varchar(50),\
                Country varchar(50),\
                Event varchar(50)'


# COMMAND ----------

insertSqlTable(athletes_transformed, 'tb_athletes_refined', columns_athletes)

insertSqlTable(coaches_transformed, 'tb_coaches_refined', columns_coaches) 

insertSqlTable(entriesgender_transformed, 'tb_entriesgender_refined', columns_entriesgender) 

insertSqlTable(medals_transformed, 'tb_medals_refined', columns_medals)

insertSqlTable(teams_transformed, 'tb_teams_refined', columns_teams) 
