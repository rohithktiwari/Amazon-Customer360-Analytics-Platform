# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

storage_account = "amazoncustomer360sa"
container = "customer360"

bronze_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/bronze/"

# COMMAND ----------

display(dbutils.fs.ls(bronze_path))

# COMMAND ----------

crm_df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv(bronze_path + "CRM.csv")
)

# COMMAND ----------

display(crm_df)

# COMMAND ----------

crm_df.printSchema()

# COMMAND ----------

crm_df.count()

# COMMAND ----------

len(crm_df.columns)

# COMMAND ----------

crm_df.select(
    [count(when(col(c).isNull(), c)).alias(c) for c in crm_df.columns]
).display()

# COMMAND ----------

crm_df.groupBy("customer_id") \
      .count() \
      .filter(col("count") > 1) \
      .display()

# COMMAND ----------

spark.sql("SHOW CATALOGS").display()

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN amazon_customer360").display()

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS amazon_customer360.bronze
""")

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS amazon_customer360.silver
""")

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS amazon_customer360.gold
""")

# COMMAND ----------

spark.sql("""
SHOW SCHEMAS IN amazon_customer360
""").display()

# COMMAND ----------

spark.sql("USE CATALOG amazon_customer360")
spark.sql("USE SCHEMA bronze")

# COMMAND ----------

crm_df.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("crm")

# COMMAND ----------

display(spark.table("crm"))

# COMMAND ----------

display(spark.table("crm"))

# COMMAND ----------

spark.sql("""
DESCRIBE DETAIL crm
""").display()

# COMMAND ----------

def ingest_to_bronze(file_name, table_name):
    
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(bronze_path + file_name)
    )

    print(f"Rows in {file_name}: {df.count()}")

    df.write \
      .mode("overwrite") \
      .format("delta") \
      .saveAsTable(table_name)

    print(f"{table_name} created successfully.")

# COMMAND ----------

ingest_to_bronze("Transactions.csv", "transactions")
ingest_to_bronze("Products.csv", "products")
ingest_to_bronze("Support_Tickets.csv", "support_tickets")
ingest_to_bronze("Marketing.csv", "marketing")

# COMMAND ----------

spark.sql("""
SHOW TABLES IN amazon_customer360.bronze
""").display()

# COMMAND ----------

tables = ["crm", "transactions", "products", "support_tickets", "marketing"]

for table in tables:
    count = spark.table(table).count()
    print(f"{table} : {count}")

# COMMAND ----------

