# Customer360 data engineering project

## Overview
This project built a ETL pipeline, ultilized PySpark to load customer data from HDFS, transform them into OLAP ouput, and load them into RDS MySQL. PowerBI is used to create dashboard whose data are loaded from the database.

![project pipeline](https://github.com/hien2706/Customer360/blob/main/pictures/Customer360_data_pipeline.png)

Technologies used:
- HDFS
- PySpark
- AWS RDS MySQL
- PowerBI

### Project Details

#### Extract
PySpark is used to extract data from HDFS. There are 2 types of data: interaction data and behavior data

#### Transform
PySpark is ultilized to transform data
##### interaction data
Script used: log_content_ETL.py
Steps included:
- categorize AppName column and calculate total duration of each contract has with each category
- Calculate total devices that each Contract uses
- Calculate what type does each Contract watch the most
- Calculate what types does each Contract watch
- Calculate the activeness of each Contract
- Segment Customers into categories

Table before:

Table after:
