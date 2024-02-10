# Project 2: Data Warehouse

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

## Requested Architecture

<p align="center">
  <img src="images/architecture.png" alt="Sparkify S3 Requested Architecture" width=60% height=60%>
</p>

# ETL Pipeline

An RDS cluster is spun up and data is raw data is moved from the designated S3 bucket into Redshift for staging. Staging data is then transformed as needed to be copied into appropriate Fact and Dimension tables to enable company reporting and analysis.

<p align="center">
  <img src="images/ETL.png" alt="Sparkify S3 to Redshift ETL" width=60% height=60%>
</p>

## Supporting Files
1. `sql_queries.py`: File that houses all queries for dropping and creating tables as well as copying and inserting data into tables.
2. `redshift_utils.py`: A module created to house common functions to reduce amount of repeated code in main notebook.
    - `create_role`: Provisions IAM role for RDS cluster.
    - `create_cluster`: Provisions RDS cluster.
    - `execute_sql_queries`: Submits query via database connection.
    - `execute_qa_count_queries`: Executes a table count on all tables provided.
    - `execute_qa_row_queries`: Executes a row of 1 query on all tables provided. 

## Driver Files
1. `create_tables.py`: Creates all tables that will be loaded by the ETL process.
2. `etl.py`: Runs the ETL process of transferring and copying data from S3 into Redshift tables.



# Tables
Below is an ERD representing the relationship between all tables created as part of the ETL process.

<p align="center">
  <img src="images/Sparkify_DB.png" alt="Sparkify Dimensional Model" width=100% height=100% >
</p>

## Staging

1. `staging_events`: All log data from Sparkify.
2. `staging_songs`: All song data from Sparkify.

## Dimensional Model (DIMM)
### Facts
1. `songplays`: A table with all song plays by users on Sparkify.

### Dimensions
1. `users`: A table for all Sparkify users.
2. `artists`: A table for all artists on Sparkify.
3. `songs`: A table for all songs on Sparkify.
4. `time`: A table for housing all time increments relevant for reporting at Sparkify.

# Analysis