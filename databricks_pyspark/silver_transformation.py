# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### CUSTOMERS

# COMMAND ----------

df_cust = spark.read.table("dbtpysparkproject.bronze.customers")

# COMMAND ----------

# DBTITLE 1,transformation to get only the domain names of each email id
df_cust = df_cust.withColumn("domain", split(col('email'),'@')[1])

# COMMAND ----------

# DBTITLE 1,transform the phone number with just numbers and remove additional characters using regexp
df_cust = df_cust.withColumn("phone_number", regexp_replace("phone_number",r"[^0-9]", ""))

# COMMAND ----------

# DBTITLE 1,transform the first & last name into 1 col & drop the first & last name cols
df_cust = df_cust.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
df_cust = df_cust.drop("first_name", "last_name")

# COMMAND ----------

import os, sys

current_dir = os.getcwd()

# COMMAND ----------

sys.path.append(current_dir)

# COMMAND ----------

# DBTITLE 1,function to remove deuplicate values
class transformations:
    # window function for depuplication
    def dedup(self, df:DataFrame, dedup_cols: List, cdc:str):
        # 1. get the hash of all the cols that are provided in the dedup_cols and concat it
        df = df.withColumn("dedupKey", concat(*dedup_cols))
        #2. count the number of deduplication, how many deduplication we have using window function
        # row number function is used to eliminate duplicates 
        df = df.withColumn("dedupCounts", row_number().over(Window.partitionBy("dedupKey").orderBy(desc(cdc))))
        #3. get the values in dedupCounts that are equal to 1, rest all should be dropped
        df = df.filter(col('dedupCounts')==1)
        df = df.drop("dedupKey", "dedupCounts")
        return df

    def process_timestamp(self, df):
        df = df.withColumn("process_timestamp", current_timestamp())
        return df
    
    def upsert(self, df, key_cols, table, cdc):
        # like a join condition, where trg & src col are same and join them if there are more cols with AND
        merge_cond = " AND ".join([f"trg.{col} = src.{col}" for col in key_cols])
        # for upsert, we need to merge the existing table with new source table
        # therefore, create a dataframe for the target table i.e table that is already present in catalog
        dlt_trg = DeltaTable.forName(spark, f"dbtpysparkproject.silver.{table}")
        # now merge this trg table with source dataframe that is coming with new data via df
        #condition is applied in case for backfilling. coz still we need latest data not backfilling old data
        dlt_trg.alias("trg").merge(df.alias('src'), merge_cond)\
                    .whenMatchedUpdateAll(condition=f"src.{cdc} >= trg.{cdc}")\
                    .whenNotMatchedInsertAll()\
                    .execute()
        return 1


# COMMAND ----------

cust_obj = transformations()

# COMMAND ----------


# the col where we need to check for deupliates is customer_id
df_cust = cust_obj.dedup(df_cust, ['customer_id'], 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %md
# MAGIC we need to load the data from bronze only which are new or updated/modified and we dont want duplicate values. 
# MAGIC so thats why we use orderBy timestamp value in descending order in function dedup(), thats how we get the latest data of existing record.

# COMMAND ----------

# DBTITLE 1,upsert logic

# if the table does not exist, then create table and initial load
if not spark.catalog.tableExists("dbtpysparkproject.silver.customers"):
    df_cust.write.format('delta').mode("append").saveAsTable("dbtpysparkproject.silver.customers")

else:
    # if table exists, then upsert the data based on the class function
    cust_obj.upsert(df_cust, ['customer_id'], 'customers', 'last_updated_timestamp')


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dbtpysparkproject.silver.customers

# COMMAND ----------

# MAGIC %md
# MAGIC ### DRIVERS

# COMMAND ----------

df_driver = spark.read.table("dbtpysparkproject.bronze.drivers")

# COMMAND ----------

df_driver = df_driver.withColumn("phone_number", regexp_replace("phone_number",r"[^0-9]", ""))


# COMMAND ----------

df_driver = df_driver.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
df_driver = df_driver.drop("first_name", "last_name")

# COMMAND ----------

df_driver.display()

# COMMAND ----------

driver_obj = transformations()

# COMMAND ----------

df_driver = driver_obj.dedup(df_driver,["driver_id"], "last_updated_timestamp")

# COMMAND ----------

df_driver = driver_obj.process_timestamp(df_driver)

# COMMAND ----------

if not spark.catalog.tableExists("dbtpysparkproject.silver.drivers"):
    df_driver = df_driver.write.format('delta').mode("append").saveAsTable("dbtpysparkproject.silver.drivers")
else:
    driver_obj.upsert(df_driver, ["driver_id"], "drivers", "last_updated_timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC ### LOCATIONS

# COMMAND ----------

df_loc = spark.read.table("dbtpysparkproject.bronze.locations")

# COMMAND ----------

df_loc.display()

# COMMAND ----------

df_loc = df_loc.withColumn("address", concat_ws(", ", col('city'), col('state'), col('country')))
df_loc = df_loc.drop('city', 'country', 'state')

# COMMAND ----------

locations_obj = transformations()

# COMMAND ----------

df_loc = locations_obj.dedup(df_loc, ['location_id'], 'last_updated_timestamp')
df_loc = locations_obj.process_timestamp(df_loc)

# COMMAND ----------

if not spark.catalog.tableExists("dbtpysparkproject.silver.locations"):
    df_loc.write.format('delta').mode("append").saveAsTable("dbtpysparkproject.silver.locations")
else:
    locations_obj.upsert(df_loc, ["location_id"], "locations", "last_updated_timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PAYMENTS

# COMMAND ----------

df_pay = spark.read.table("dbtpysparkproject.bronze.payments")

# COMMAND ----------

df_pay.display()

# COMMAND ----------

df_pay = df_pay.withColumn("online_payment_status", \
                when((col('payment_method') == 'Card') & (col('payment_status') == 'Success'), "online-success")\
                .when((col('payment_method') == 'Card') & (col('payment_status') == 'Failed'), "online-failed")\
                .when((col('payment_method') == 'Card') & (col('payment_status') == 'Pending'), "online-pending")\
                .otherwise("offline"))

# COMMAND ----------

payments_obj = transformations()

# COMMAND ----------

df_pay = payments_obj.dedup(df_pay, ['payment_id'], 'last_updated_timestamp')
df_pay = payments_obj.process_timestamp(df_pay)

# COMMAND ----------

if not spark.catalog.tableExists("dbtpysparkproject.silver.payments"):
    df_pay.write.format('delta').mode('append').saveAsTable('dbtpysparkproject.silver.payments')
else:
    payments_obj.upsert(df_pay, ['payment_id'], 'payments', "last_updated_timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRIPS

# COMMAND ----------

df_trips = spark.read.table('dbtpysparkproject.bronze.trips')

# COMMAND ----------

df_trips.display()

# COMMAND ----------

trip_obj = transformations() 

# COMMAND ----------

df_trips = trip_obj.dedup(df_trips, ['trip_id'], 'last_updated_timestamp')
df_trips = trip_obj.process_timestamp(df_trips)

# COMMAND ----------

if not spark.catalog.tableExists("dbtpysparkproject.silver.trips"):
    df_trips.write.format('delta').mode('append').saveAsTable("dbtpysparkproject.silver.trips")
else:
    trip_obj.upsert(df_trips, ['trip_id'], "trips", "last_updated_timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC ### VEHICLES

# COMMAND ----------

df_vehichle = spark.read.table('dbtpysparkproject.bronze.vehicles')

# COMMAND ----------

df_vehichle.display()

# COMMAND ----------

df_vehichle = df_vehichle.withColumn("make", upper(col('make')))

# COMMAND ----------

veh_obj = transformations() 

# COMMAND ----------

df_vehichle = veh_obj.dedup(df_vehichle, ['vehicle_id'], 'last_updated_timestamp')

# COMMAND ----------

df_vehichle = veh_obj.process_timestamp(df_vehichle)

# COMMAND ----------

if not spark.catalog.tableExists('dbtpysparkproject.silver.vehicles'):
    df_vehichle.write.format('delta').mode('append').saveAsTable('dbtpysparkproject.silver.vehicles')
else:
    veh_obj.upsert(df_vehichle, ['vehicle_id'], 'vehicles', 'last_updated_timestamp')