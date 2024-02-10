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


# Tables
Below is an ERD representing the relationship between all tables created as part of the ETL process.

<p align="center">
  <img src="images/Sparkify_DB.png" alt="Sparkify Dimensional Model" width=100% height=100% >
</p>

## Staging

1. `staging_events`:
2. `staging_songs`:

## Dimensional Model (DIMM)
### Facts
1. `songplays`:

### Dimensions
1. `users`:
2. `artists`:
3. `songs`:
4. `time`: