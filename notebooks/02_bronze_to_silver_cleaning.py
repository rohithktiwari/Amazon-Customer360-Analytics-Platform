# Databricks notebook source
# MAGIC %md
# MAGIC # Read Bronze Tables

# COMMAND ----------

transactions_df = spark.read.table("amazon_customer360.bronze.transactions")
products_df = spark.read.table("amazon_customer360.bronze.products")
support_df = spark.read.table("amazon_customer360.bronze.support_tickets")
marketing_df = spark.read.table("amazon_customer360.bronze.marketing")

# COMMAND ----------

# MAGIC %md
# MAGIC # Validate Datasets

# COMMAND ----------

print("Transactions :", transactions_df.count())
print("Products :", products_df.count())
print("Support :", support_df.count())
print("Marketing :", marketing_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC # Remove Duplicates

# COMMAND ----------

transactions_df = transactions_df.dropDuplicates(["Transaction_ID"])
products_df = products_df.dropDuplicates(["Product_ID"])
support_df = support_df.dropDuplicates(["Ticket_ID"])
marketing_df = marketing_df.dropDuplicates(["Campaign_ID"])

# COMMAND ----------

# MAGIC %md
# MAGIC # Handle Missing Values

# COMMAND ----------

transactions_df = transactions_df.fillna("Unknown")
products_df = products_df.fillna("Unknown")
support_df = support_df.fillna("Unknown")
marketing_df = marketing_df.fillna("Unknown")

# COMMAND ----------

# MAGIC %md
# MAGIC # Save Silver Tables

# COMMAND ----------

spark.sql("USE SCHEMA silver")

transactions_df.write.mode("overwrite").format("delta").saveAsTable("transactions")

products_df.write.mode("overwrite").format("delta").saveAsTable("products")

support_df.write.mode("overwrite").format("delta").saveAsTable("support_tickets")

marketing_df.write.mode("overwrite").format("delta").saveAsTable("marketing")

# COMMAND ----------

# MAGIC %md
# MAGIC # Verify Silver Tables

# COMMAND ----------

spark.sql("""
SHOW TABLES IN amazon_customer360.silver
""").display()

# COMMAND ----------

spark.sql("""
SHOW TABLES IN amazon_customer360.bronze
""").display()

# COMMAND ----------

spark.sql("USE SCHEMA silver")

transactions_df.write.mode("overwrite").format("delta").saveAsTable("transactions")

products_df.write.mode("overwrite").format("delta").saveAsTable("products")

support_df.write.mode("overwrite").format("delta").saveAsTable("support_tickets")

marketing_df.write.mode("overwrite").format("delta").saveAsTable("marketing")

# COMMAND ----------

spark.sql("""
SHOW TABLES IN amazon_customer360.silver
""").display()

# COMMAND ----------

spark.sql("""
SELECT current_catalog(), current_schema()
""").display()

# COMMAND ----------

spark.sql("""
SHOW TABLES IN amazon_customer360.bronze
""").display()

# COMMAND ----------

transactions_df.count()
products_df.count()
support_df.count()
marketing_df.count()

# COMMAND ----------

transactions_df.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("amazon_customer360.silver.transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC # Save Remaining Silver Tables

# COMMAND ----------

products_df.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("amazon_customer360.silver.products")

support_df.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("amazon_customer360.silver.support_tickets")

marketing_df.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("amazon_customer360.silver.marketing")

# COMMAND ----------

# MAGIC %md
# MAGIC # Verify Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC