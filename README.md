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
- Categorize the `AppName` column and calculate the total duration each contract has with each category.
- Calculate the total devices each contract uses.
- Identify the type each contract watches the most.
- Determine the various types each contract watches.
- Assess the activeness of each contract.
- Segment Customers into categories.

Before Transformation:\
![interaction_data_before](https://github.com/hien2706/Customer360/blob/main/pictures/interaction_data_before.png)\
After Transformation:\
![interaction_data_after](https://github.com/hien2706/Customer360/blob/main/pictures/interaction_data_after.png)

##### Behavior data
Script used: `log_search_ETL.py` \
Steps included:
- Filter out NULL values and data from months 6 and 7.
- Identify the most searched keyword for each `user_id` in months 6 and 7.
- Categorize the most searched keyword for each user.
- Calculate a new column `Trending_Type` to identify if the category changed or remained unchanged within 2 months.
- Calculate a new column `Previous` to show any changes in category within 2 months, if applicable.
Before Transformation:\
![behavior_data_before](https://github.com/hien2706/Customer360/blob/main/pictures/behavior_data_before.png)\
After Transformation:\
![behavior_data_after](https://github.com/hien2706/Customer360/blob/main/pictures/behavior_data_after.png)
#### Load
The transformed data is loaded into RDS MySQL. There are two tables: `customer_behavior_data` and `customer_interaction_data`.

#### Analyze
PowerBI is used to create dashboards based on the data loaded from RDS MySQL.\
![Dashboard](https://github.com/hien2706/Customer360/blob/main/pictures/customer_data.pdf):\
![DashBoard](https://github.com/hien2706/Customer360/blob/main/pictures/dashboard.png)
