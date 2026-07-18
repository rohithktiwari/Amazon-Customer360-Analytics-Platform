# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Customer 360 Gold Layer

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Silver Tables

# COMMAND ----------

print("CRM :", crm_df.count())
print("Transactions :", transactions_df.count())
print("Products :", products_df.count())
print("Support :", support_df.count())
print("Marketing :", marketing_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Customer 360 Master Table

# COMMAND ----------

customer360_df = (
    crm_df.alias("c")
    .join(transactions_df.alias("t"), "Customer_ID", "left")
    .join(products_df.alias("p"), "Product_ID", "left")
    .join(support_df.alias("s"), "Customer_ID", "left")
    .join(marketing_df.alias("m"), "Customer_ID", "left")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Customer 360

# COMMAND ----------

display(customer360_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Gold Table

# COMMAND ----------

spark.sql("USE SCHEMA gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Gold Table

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

spark.sql("USE CATALOG amazon_customer360")

crm_df = spark.table("amazon_customer360.silver.crm")

transactions_df = spark.table("amazon_customer360.silver.transactions")

products_df = spark.table("amazon_customer360.silver.products")

support_df = spark.table("amazon_customer360.silver.support_tickets")

marketing_df = spark.table("amazon_customer360.silver.marketing")

# COMMAND ----------

spark.sql("USE SCHEMA gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM amazon_customer360.silver.crm

# COMMAND ----------

# Customer 360 Gold Layer

spark.sql("USE CATALOG amazon_customer360")

# Read Silver Tables
crm_df = spark.table("amazon_customer360.silver.crm")
transactions_df = spark.table("amazon_customer360.silver.transactions")
products_df = spark.table("amazon_customer360.silver.products")
support_df = spark.table("amazon_customer360.silver.support_tickets")
marketing_df = spark.table("amazon_customer360.silver.marketing")

# Validate
print("CRM :", crm_df.count())
print("Transactions :", transactions_df.count())
print("Products :", products_df.count())
print("Support :", support_df.count())
print("Marketing :", marketing_df.count())

# Create Customer360
customer360_df = (
    crm_df.alias("c")
    .join(transactions_df.alias("t"), "Customer_ID", "left")
    .join(products_df.alias("p"), "Product_ID", "left")
    .join(support_df.alias("s"), "Customer_ID", "left")
    .join(marketing_df.alias("m"), "Customer_ID", "left")
)

# Preview
display(customer360_df)

# Save Gold Table
spark.sql("USE SCHEMA gold")

customer360_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("customer360")

# Verify
display(
    spark.table("amazon_customer360.gold.customer360")
)

# COMMAND ----------

customer360_df = (
    crm_df.alias("c")
    .join(transactions_df.alias("t"), "Customer_ID", "left")
    .join(products_df.alias("p"), "Product_ID", "left")
    .join(support_df.alias("s"), "Customer_ID", "left")
    .join(marketing_df.alias("m"), "Customer_ID", "left")
    .select(
        "c.*",
        "t.Order_ID",
        "t.Product_ID",
        "t.Qty",
        "t.Amount",
        "t.Payment",
        "p.Product_Name",
        "p.Category",
        "p.Brand",
        "p.Price",
        "s.Ticket_ID",
        "s.Issue",
        "s.Priority",
        "m.Campaign_ID",
        "m.Campaign",
        "m.Channel"
    )
)

# COMMAND ----------

support_df.printSchema()

# COMMAND ----------

