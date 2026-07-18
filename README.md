# 🚀 Amazon Customer360 Analytics Platform

A production-inspired Azure Data Engineering project that integrates customer information from multiple business systems into a unified **Customer360 Gold Layer** using the Medallion Architecture.

---

## 📖 Project Overview

The objective of this project is to build an end-to-end ETL pipeline that collects customer-related data from different business domains, transforms it through Bronze, Silver, and Gold layers, and creates a single Customer360 dataset for analytics and reporting.

---

## 🎯 Business Problem

Customer information is distributed across multiple systems including CRM, Transactions, Products, Support Tickets, and Marketing.

This fragmented data makes it difficult for organizations to understand customer behavior and generate business insights.

This project solves the problem by creating a unified Customer360 dataset.

---

<img width="1536" height="722" alt="Screenshot 2026-07-18 111639" src="https://github.com/user-attachments/assets/1f0a1e98-9443-4a86-8c86-7c016c4eca3d" />


---

## 🔄 ETL Workflow

```text
CSV Files
      │
      ▼
Azure Data Lake Storage Gen2
      │
      ▼
Azure Databricks (PySpark)
      │
      ▼
Bronze Layer
      │
      ▼
Silver Layer
      │
      ▼
Gold Layer (Customer360)
      │
      ▼
Azure Data Factory Pipeline
      │
      ▼
Business Analytics
```

---

## 🗂️ Source Datasets

| Dataset | Description |
|----------|-------------|
| 👤 CRM | Customer profile information |
| 🛒 Transactions | Purchase history |
| 📦 Products | Product catalog |
| 🎫 Support | Customer support interactions |
| 📢 Marketing | Campaign engagement |

---

## 🥉 Bronze Layer

- Raw CSV ingestion
- Delta table creation
- No transformations

---

## 🥈 Silver Layer

- Data cleaning
- Schema validation
- Duplicate handling
- Null value processing
- Standardization

---

## 🥇 Gold Layer

Customer360 dataset containing

- Customer Profile
- Transaction Summary
- Support Summary
- Marketing Summary

One row represents one customer.

---

## ⚙️ Technologies Used

| Category | Technology |
|----------|------------|
| Cloud | Azure |
| Storage | Azure Data Lake Storage Gen2 |
| Processing | Azure Databricks |
| Language | PySpark |
| Data Format | Delta Lake |
| Catalog | Unity Catalog |
| Orchestration | Azure Data Factory |
| Query Language | SQL |

---

## 💻 SQL Concepts Used

- SELECT
- WHERE
- GROUP BY
- LEFT JOIN
- SUM()
- COUNT()
- FIRST()
- Aggregations
- Validation Queries

---

## 📁 Repository Structure

```text
Amazon-Customer360-Analytics-Platform/
│
├── architecture/
├── documentation/
├── notebooks/
├── screenshots/
├── sql/
├── README.md
└── LICENSE
```

---

## 📸 Project Screenshots

### Azure Resources

> Insert screenshot

### Databricks Workspace

> Insert screenshot

### Bronze Tables

> Insert screenshot

### Silver Tables

> Insert screenshot

### Gold Layer

> Insert screenshot

### Azure Data Factory Pipeline

> Insert screenshot

---

## 🚀 Project Highlights

- End-to-End Azure Data Engineering Pipeline
- Medallion Architecture
- Delta Lake
- Customer360 Gold Layer
- Azure Data Factory Orchestration
- Enterprise Data Modeling

---

## 📈 Future Improvements

- Power BI Dashboard
- Incremental Data Loading
- CI/CD Pipeline
- Data Quality Monitoring
- Azure Key Vault Integration

---

## 👨‍💻 Author

**Rohit**

Aspiring Data Engineer
<p align="center">

<img src="https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white"/>

<img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white"/>

<img src="https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>

<img src="https://img.shields.io/badge/Delta%20Lake-003B57?style=for-the-badge"/>

<img src="https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white"/>

<img src="https://img.shields.io/badge/Azure%20Data%20Factory-0062AD?style=for-the-badge"/>

</p>
