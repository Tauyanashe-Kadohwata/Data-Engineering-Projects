Mobile Game In-App Purchase Analytics (End-to-End Azure Pipeline)
Project Overview
This project demonstrates an end-to-end Data Engineering ETL pipeline built on Microsoft Azure. The goal was to analyze mobile game in-app purchase behavior to identify top revenue-generating countries and player spending habits.

Tech Stack:

Storage: Azure Data Lake Storage Gen2 (ADLS)

Transformation: Azure Databricks (PySpark)

Orchestration: Azure Data Factory (ADF)

Data Warehousing: Azure SQL Database

Visualization: Power BI

Security: Azure Key Vault

Architecture & Workflow
Ingestion (Bronze): Raw JSON logs (mobile_game_inapp_purchases.json) containing user transactions were ingested into the ADLS Bronze container.

Transformation (Silver): * Mounted ADLS to Databricks using secure Key Vault secrets.

Cleaned data using PySpark (fixed timestamp formats, casted data types).

Filtered out corrupt records (Null UserIDs).

Loaded cleaned data into the Silver container as flat Parquet files.

Loading (Gold/Serving): Azure Data Factory orchestrated the movement of data from the Silver layer into an Azure SQL Database.

Reporting: Power BI connected to the SQL Database to visualize the Top 10 Countries by Revenue and player demographics.

Challenges & Bottlenecks (The "Learning" Section)
During the development of this pipeline, several technical bottlenecks were encountered and resolved:

1. Security & Access Control
Issue: Databricks Secret Scope creation failed due to "Premium Tier" restrictions on the workspace.

Solution: utilized the "DNS Name" and "Resource ID" method to create a specific scope for the individual user (users principal) instead of "All Users," bypassing the tier restriction.

2. Data Lake Connectivity
Issue: PathNotFoundException when Databricks tried to read from storage.

Root Cause: The connection string used the .blob.core.windows.net endpoint, which conflicts with the ADLS Gen2 driver.

Solution: Updated the URI to use the correct .dfs.core.windows.net endpoint required for abfss:// protocol.

3. Data Quality & Schema Enforcement
Issue: The SQL Load failed with Cannot insert NULL into column 'UserID'.

Root Cause: The raw JSON contained missing User IDs, but the SQL destination required a Primary Key (Not Null).

Solution: Implemented a .filter(col("UserID").isNotNull()) transformation step in PySpark to quarantine bad data before loading.

4. Orchestration & File Formats
Issue: ADF failed with ParquetInvalidFile error.

Root Cause: Saving data as "Delta" created hidden _delta_log JSON files mixed with Parquet data. ADF tried to read the JSON logs as Parquet files.

Solution: Configured ADF Source to use a Wildcard path (*.parquet) to selectively read only data files and ignore system logs.
