# EcommerceFlow

End-to-end ETL pipeline for ecommerce data using Azure stack.

## Architecture
Raw CSV → ADLS Bronze → Databricks Silver → Databricks Gold → Snowflake → Power BI

## Tech Stack
- Azure Data Factory (ADF) - Orchestration
- Azure Databricks - PySpark Transformations
- ADLS Gen2 - Data Lake Storage
- Snowflake - Data Warehouse
- Delta Lake - Storage Format
- Git - Version Control

## Dataset
Olist Brazilian E-Commerce (100k+ orders, 9 tables)

## Project Structure
- databricks/ - PySpark notebooks
- snowflake/ - DDL, views, stored procedures
- adf/ - Pipeline configs
- sql/ - Quality checks
- data/ - Raw CSV files
