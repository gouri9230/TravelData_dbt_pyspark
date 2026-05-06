# Databricks notebook source
df = spark.read.format('csv').option("header", True).option("inferSchema", True)\
                        .load("/Volumes/dbtpysparkproject/source/source_data/customers/")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPARK STREAMING

# COMMAND ----------

entities = ['customers','trips','locations','payments','drivers','vehicles']

# COMMAND ----------

for entity in entities:
    # batch read to get the schema
    df_for_schema = spark.read.format('csv')\
                .option("header", True)\
                .option("inferSchema", True)\
                .load(f"/Volumes/dbtpysparkproject/source/source_data/{entity}/")
    # store schema of each table 
    entity_schema = df_for_schema.schema

    # dynamic stream reading of each table 
    df = spark.readStream.format('csv')\
            .option("header", True)\
            .schema(entity_schema)\
            .load(f"/Volumes/dbtpysparkproject/source/source_data/{entity}/")
    
    # write incremental data of each table using checkpoint
    df.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation", f"/Volumes/dbtpysparkproject/bronze/checkpoint/{entity}")\
    .trigger(once=True)\
    .toTable(f"dbtpysparkproject.bronze.{entity}")