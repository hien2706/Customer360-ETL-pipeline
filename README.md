# Customer360 data engineering project

## Overview
This project involves building an ETL pipeline using PySpark to load customer data from HDFS, transform it into OLAP outputs, and load it into an RDS MySQL database. PowerBI is used to create dashboards based on the data loaded from the database.

![project pipeline](https://github.com/hien2706/Customer360/blob/main/pictures/Customer360_data_pipeline.png)

Technologies used:
- HDFS
- PySpark
- AWS RDS MySQL
- PowerBI

### Project Details

#### Extract
PySpark is used to extract two types of data from HDFS: interaction data and behavior data.

#### Transform
PySpark is utilized to transform the data.
##### Interaction data
Script used: `log_content_ETL.py` \
Steps included:
- categorize AppName column and calculate total duration of each contract has with each category
- Calculate total devices that each Contract uses
- Calculate what type does each Contract watch the most
- Calculate what types does each Contract watch
- Calculate the activeness of each Contract
- Segment Customers into categories

Table before:\
![interaction_data_before](https://github.com/hien2706/Customer360/blob/main/pictures/interaction_data_before.png)\
Table after:\
![interaction_data_after](https://github.com/hien2706/Customer360/blob/main/pictures/interaction_data_after.png)
##### Behavior data
Script used: log_search_ETL.py \
Steps included:
- Sort out NULL values, month 6 and 7
- Find Most search keyword of each user_id in month 6 and 7
- categorize most search keyword of each user
- Calculate new column Trending_Type to see if the category change or unchange within 2 months
- Calculate new column Previous to show the change in category within 2 months if it happens
Table before:\
![behavior_data_before](https://github.com/hien2706/Customer360/blob/main/pictures/behavior_data_before.png)\
Table after:\
![behavior_data_after](https://github.com/hien2706/Customer360/blob/main/pictures/behavior_data_after.png)
#### Load
data are loaded into RDS MySQL, there are two tables: customer_behavior_data, and customer_interaction_data

#### Analyze
PowerBI is used to create dashboard whose data is loaded from RDS MySQL\
![Dashboard](https://github.com/hien2706/Customer360/blob/main/pictures/customer_data.pdf):\
![DashBoard](https://github.com/hien2706/Customer360/blob/main/pictures/dashboard.png)
