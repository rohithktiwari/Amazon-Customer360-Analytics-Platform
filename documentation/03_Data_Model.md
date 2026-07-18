# 🗄️ Data Model

## 📌 Source Datasets

The Customer360 platform integrates data from five business datasets.

---

# 👤 CRM

Contains customer profile information.

### Primary Key

- Customer_ID

---

# 🛒 Transactions

Contains customer purchase history.

### Primary Key

- Transaction_ID

### Foreign Keys

- Customer_ID
- Product_ID

---

# 📦 Products

Contains product catalog information.

### Primary Key

- Product_ID

---

# 🎫 Support Tickets

Contains customer service interactions.

### Primary Key

- Ticket_ID

### Foreign Key

- Customer_ID

---

# 📢 Marketing

Contains customer campaign engagement data.

### Primary Key

- Campaign_ID

### Foreign Key

- Customer_ID

---

# 🏗️ Medallion Architecture

## 🥉 Bronze Layer

- Raw CSV data ingestion
- Original data storage
- No transformations

---

## 🥈 Silver Layer

- Data cleansing
- Data standardization
- Data quality improvements
- Delta table creation

---

## 🥇 Gold Layer

The Customer360 Gold Layer combines:

- 👤 Customer Profile
- 💰 Transaction Summary
- 🎫 Support Summary
- 📢 Marketing Summary

into a single analytics-ready dataset.

---

# 🎯 Final Output

The Gold Layer serves as the **single source of truth** for customer analytics, enabling business users to perform reporting, segmentation, and customer behavior analysis efficiently.
