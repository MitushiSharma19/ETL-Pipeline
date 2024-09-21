# ETL-Pipeline
This project includes Python scripts for ETL (Extract, Transform, Load) operations, extracting data from MySQL and PostgreSQL, transforming it (cleaning, reformatting), and loading it into Hive tables. The scripts automate data integration, making it ready for distributed analysis in Hive.

### ETL Process:
1. **Extraction**: Data is fetched from MySQL and PostgreSQL databases.
2. **Transformation**: The scripts handle data cleaning, formatting, and other transformations to ensure the data is ready for analysis.
3. **Loading**: The transformed data is saved into a Hive table, enabling efficient querying and analysis.

## Features
- Database connection management for MySQL, PostgreSQL, and Hive.
- Modular functions for extraction, transformation, and loading.
- Scalable and distributed data handling using Hive.
- Logging for process tracking and error handling.

## Prerequisites
- Python 3.x
- Libraries:
  - `mysql-connector-python`
  - `psycopg2`
  - `pandas`
  - `pyhive`
  - `sqlalchemy`


