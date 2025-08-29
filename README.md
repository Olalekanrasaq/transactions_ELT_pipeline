## End-to-End ELT Pipeline for a simulated store transactions

In this project, a pipeline was designed to move data generated from a store transactions to an amazon redshift data warehouse which is then used for the purpose of analytics.

### Choice of Design

The pipeline make use of batch processing. Transactions data accumulated over a period of 24hours were moved into data warehouse for analytics.

### Data Ingestion

This is a simulated project. The transaction data are generated using Faker library in python. The data are in four categories; customers data, items data, dates, and orders. This four data are generated with faker and are stored in csv formats. The csv files are then ingested into AWS S3 data lake. The ingestion of the generated csv files are scheduled to be ingested into S3 every 24hours.

### Data Loading

The data are copied from AWS S3 into Redshift data warehouse. Four tables each for customers, items, dates, and orders were created in redshift and the data are loaded into this table from AWS S3.

### Data Transformation

The data in the redshift warehouse are transformed using dbt. Transformation performed include joins, aggregation of sales items, and computation of necessary variables.

### Data Serving

The transformed data was loaded as a separate table into the warehouse for the purpose of queries and analytics.

### Analytics

A streamlit app was built on the pipeline containing dashboard showing KPIs and analytics of the sales generated. 

### Orchestration

The workflow of the project is orchestrated using a custom Apache airflow image specified in the docker-compose yaml file. The custom image contain basic airflow services along with AWS services needed for the project.

### Containerization

The project is facilitated with docker and git.
