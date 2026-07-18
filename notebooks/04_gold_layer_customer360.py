# Databricks notebook source
# MAGIC %md
# MAGIC # Customer 360 Gold Layer

# COMMAND ----------

# ==========================================
# Customer 360 Gold Layer
# ==========================================

spark.sql("USE CATALOG amazon_customer360")

# COMMAND ----------

crm_df = spark.table("amazon_customer360.silver.crm")
transactions_df = spark.table("amazon_customer360.silver.transactions")
products_df = spark.table("amazon_customer360.silver.products")
support_df = spark.table("amazon_customer360.silver.support_tickets")
marketing_df = spark.table("amazon_customer360.silver.marketing")

# COMMAND ----------

print("CRM :", crm_df.count())
print("Transactions :", transactions_df.count())
print("Products :", products_df.count())
print("Support :", support_df.count())
print("Marketing :", marketing_df.count())

# COMMAND ----------

transactions_df.printSchema()

# COMMAND ----------

support_df.printSchema()

# COMMAND ----------

marketing_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

spark.sql("USE CATALOG amazon_customer360")

# COMMAND ----------

crm_df = spark.table("amazon_customer360.silver.crm")
transactions_df = spark.table("amazon_customer360.silver.transactions")
products_df = spark.table("amazon_customer360.silver.products")
support_df = spark.table("amazon_customer360.silver.support_tickets")
marketing_df = spark.table("amazon_customer360.silver.marketing")

# COMMAND ----------

print("CRM :", crm_df.count())
print("Transactions :", transactions_df.count())
print("Products :", products_df.count())
print("Support :", support_df.count())
print("Marketing :", marketing_df.count())

# COMMAND ----------

customer360_df = (
    crm_df.alias("c")
    .join(transactions_df.alias("t"), "Customer_ID", "left")
    .join(products_df.alias("p"), "Product_ID", "left")
    .join(support_df.alias("s"), "Customer_ID", "left")
    .join(marketing_df.alias("m"), "Customer_ID", "left")

    .select(

        # CRM
        col("c.Customer_ID"),
        col("c.First_Name"),
        col("c.Last_Name"),
        col("c.Gender"),
        col("c.Email"),
        col("c.Phone"),
        col("c.DOB"),
        col("c.City"),
        col("c.State"),
        col("c.Country"),
        col("c.Membership"),
        col("c.Loyalty_Points"),
        col("c.Segment"),
        col("c.Income"),
        col("c.Status").alias("Customer_Status"),

        # Transactions
        col("t.Transaction_ID"),
        col("t.Order_ID"),
        col("t.Product_ID"),
        col("t.Qty"),
        col("t.Amount"),
        col("t.Payment"),
        col("t.Status").alias("Transaction_Status"),

        # Products
        col("p.Product_Name"),
        col("p.Category"),
        col("p.Brand"),
        col("p.Price"),

        # Support
        col("s.Ticket_ID"),
        col("s.Support_Order_ID"),
        col("s.Issue"),
        col("s.Priority"),
        col("s.Status").alias("Support_Status"),

        # Marketing
        col("m.Campaign_ID"),
        col("m.Campaign"),
        col("m.Channel"),
        col("m.Opened"),
        col("m.Clicked"),
        col("m.Converted")
    )
)

# COMMAND ----------

support_df.columns

# COMMAND ----------

support_df = support_df.withColumnRenamed("Order_ID", "Support_Order_ID")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import functions as F

spark.sql("USE CATALOG amazon_customer360")

crm = spark.table("amazon_customer360.silver.crm")
txn = spark.table("amazon_customer360.silver.transactions")
support = spark.table("amazon_customer360.silver.support_tickets")
marketing = spark.table("amazon_customer360.silver.marketing")

# -------------------------
# Transaction Summary
# -------------------------

txn_summary = (
    txn.groupBy("Customer_ID")
    .agg(
        count("Transaction_ID").alias("Total_Transactions"),
        sum("Qty").alias("Total_Qty"),
        sum("Amount").alias("Total_Revenue"),
        first("Payment").alias("Preferred_Payment")
    )
)

# -------------------------
# Support Summary
# -------------------------

support_summary = (
    support.groupBy("Customer_ID")
    .agg(
        count("Ticket_ID").alias("Total_Tickets"),
        first("Priority").alias("Latest_Priority")
    )
)

# -------------------------
# Marketing Summary
# -------------------------

marketing_summary = (
    marketing.groupBy("Customer_ID")
    .agg(
        sum("Opened").alias("Emails_Opened"),
        sum("Clicked").alias("Emails_Clicked"),
        sum("Converted").alias("Conversions")
    )
)

# -------------------------
# Customer360
# -------------------------

customer360 = (
    crm.alias("c")
    .join(txn_summary.alias("t"), "Customer_ID", "left")
    .join(support_summary.alias("s"), "Customer_ID", "left")
    .join(marketing_summary.alias("m"), "Customer_ID", "left")
)

display(customer360)

# COMMAND ----------

spark.sql("USE SCHEMA gold")

customer360.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("customer360")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS amazon_customer360.gold.customer360;

# COMMAND ----------

customer360.write \
.mode("overwrite") \
.option("overwriteSchema","true") \
.format("delta") \
.saveAsTable("amazon_customer360.gold.customer360")

# COMMAND ----------

display(
spark.table("amazon_customer360.gold.customer360")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) 
# MAGIC FROM amazon_customer360.gold.customer360;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM amazon_customer360.gold.customer360
# MAGIC LIMIT 20;