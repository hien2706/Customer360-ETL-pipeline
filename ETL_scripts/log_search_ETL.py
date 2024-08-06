import pandas as pd
import findspark
findspark.init()
import pyspark
from pyspark.sql.functions import *
from pyspark.sql import functions as sf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import os
from datetime import datetime


spark = (SparkSession.builder
         .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.30")
         .config("spark.driver.memory", "4g")
         .getOrCreate())

def filter_null_duplicates_and_month(df):
    df = df.filter(col("user_id").isNotNull()).filter(col("keyword").isNotNull())
    df = df.filter((col("month") == 6) | (col("month") == 7))

    return df


def find_most_searched_keyword(df):
    keyword_counts = df.groupBy("month","user_id","keyword").count()
    window_spec = Window.partitionBy("month", "user_id").orderBy(col("count").desc())
    keyword_counts_with_rank = keyword_counts.withColumn("rank", row_number().over(window_spec))
    most_keyword_counts = keyword_counts_with_rank.filter(col("rank") == 1).select("month", "user_id", "keyword")
    return most_keyword_counts

def get_most_searched_keywords_trimmed(df, month1, month2):
    most_keyword_month_1 = df.filter(col("month") == month1).withColumnRenamed("keyword", f"most_search_month_{month1}").select("user_id", f"most_search_month_{month1}")
    most_keyword_month_2 = df.filter(col("month") == month2).withColumnRenamed("keyword", f"most_search_month_{month2}").select("user_id", f"most_search_month_{month2}")
    
    final_result = most_keyword_month_1.join(most_keyword_month_2, on="user_id", how="inner")
    final_result = final_result.withColumn(f"most_search_month_{month1}", trim(col(f"most_search_month_{month1}")))
    final_result = final_result.withColumn(f"most_search_month_{month2}", trim(col(f"most_search_month_{month2}")))
    
    return final_result.limit(250)

def get_search_category(df, mapping_df):
    df = df.alias("df").join(
        mapping_df.alias("mapping_t6"),
        col("df.most_search_month_6") == col("mapping_t6.search"),
        "left"
    ).select(
        col("df.*"),
        col("mapping_t6.category").alias("category_t6")
    )
    
    df = df.alias("df").join(
        mapping_df.alias("mapping_t7"),
        col("df.most_search_month_7") == col("mapping_t7.search"),
        "left"
    ).select(
        col("df.*"),
        col("mapping_t7.category").alias("category_t7")
    )
    return df
def get_Trending_type_column(df):
    return df.withColumn("Trending_Type", 
                                       when(col("category_t6") == col("category_t7"), "Unchanged").otherwise("Changed"))

def get_Previous_column(df):
    return df.withColumn("Previous",
                                    when(col("category_t6") == col("category_t7"), "Unchanged").otherwise(concat_ws(" -> ", col("category_t6"), col("category_t7"))))

def write_to_MySQL(df,host,port,database_name,table,user,password):
    df.write.format("jdbc").options(
        url=f"jdbc:mysql://{host}:{port}/{database_name}",
        driver = "com.mysql.cj.jdbc.Driver",
        dbtable = table,
        user= user,
        password= password).mode('overwrite').save()    
    
    
def main(path_to_read,path_to_save,path_of_mapping_file):
    # print('-------------Validating paths--------------')
    # if not os.path.exists(path_to_read):
    #     return print(f"{path_to_read} does not exist to read from")
    # if os.path.exists(path_to_save):
    #     return print(f"{path_to_save} already exists")
    print('-------------Finding parquet files from path--------------')
    
    
    
    parquet_files = os.listdir(path_to_read)
    parquet_files = [os.path.join(path_to_read , file) for file in parquet_files]
    
    if not parquet_files:
        return print(f"No parquet files found in the path {path_to_read}")
    
    print('-------------Reading and Unionizing parquet files--------------')
    df_union = None
    for file in parquet_files:
        df = spark.read.parquet(file)
        df = df.select(month(to_date(df.datetime)).alias("month"), "user_id", "keyword")
        if df is not None:
            if df_union is None:
                df_union = df
            else:
                df_union = df_union.unionByName(df)
                df_union = df_union.cache()
                
    if df_union is None:
        return print('No DataFrames created from parquet files.')
    
    print('Filtering out rows with null values, duplicates, and columns not in month 6 and 7')
    
    df_union = filter_null_duplicates_and_month(df_union)
    
    print('Find most searched keyword for each user')
    df_union = find_most_searched_keyword(df_union)
    
    print('Find most searched keyword for each user in month 6 and 7') 
    df_most_searched_keyword = get_most_searched_keywords_trimmed(df_union, 6, 7)
    
    print('Reading mapping file')
    mapping_df = spark.read.csv(path_of_mapping_file, header=True).dropDuplicates(["search"])
    
    
    print('Get search category')
    df_most_searched_keyword = get_search_category(df_most_searched_keyword, mapping_df)
    
    print('Get Trending Type column')
    df_most_searched_keyword = get_Trending_type_column(df_most_searched_keyword)
    
    print('Get Previous column')
    df_most_searched_keyword = get_Previous_column(df_most_searched_keyword)
    
    print('Preview of the final result')
    df_most_searched_keyword.show(truncate=False)
    print(df_most_searched_keyword)    
    
    # print('Saving the final result to csv file at path:', path_to_save)
    # df_most_searched_keyword.repartition(1).write.csv(path_to_save,
    #                                        #mode = 'overwrite',
    #                                        header = True)
    
    check_flag = input('Write to mysql? (Y/n): ')
    if check_flag.lower() != 'y':
        return print('Task finished')
    # host = input('enter hostname:')
    # port = input('enter port:')
    # user = input('enter user:')
    # password = input('enter password:')
    # database_name = input('enter database name:')
    # table = input('enter table name:')
    
    # # MySQL credentials 
    # port = 3306
    # host = "localhost"
    # database_name = 'usr_activities'
    # table = 'customer_behavior_data'
    # user = 'hien2706'
    # password = '5conmeocoN@'
    
    # RDS MySQL credentials
    host = "customer360-db.c1uy4eie6gye.ap-southeast-1.rds.amazonaws.com"
    port = 3306
    database_name = "customer_data"
    table = "customer_behavior_data"
    user = "admin"
    password = "5conmeocon"
    
    print('-------------Loading result into MySql db--------------')
    write_to_MySQL(df = df_most_searched_keyword,
                host = host,
                port= port,
                database_name = database_name,
                table = table,
                user = user,
                password = password) 
    
    return print('Task finished')
    


if __name__ == "__main__":
    # path_to_read = input("Enter the path to read data: ")
    # path_to_save = input("Enter the path to save results: ")
    # path_of_mapping_file = input("Enter the path of mapping file: ")
    
    path_to_read = "/home/hien2706/hien_data/lop_DE/log_search"
    path_of_mapping_file = "/home/hien2706/hien_code/lop_DE/class_3/mapping.csv"
    path_to_save = "/home/hien2706/hien_data/lop_DE/output_log_search"
    

    main(path_to_read = path_to_read, 
        path_to_save = path_to_save,
        path_of_mapping_file = path_of_mapping_file)

#/home/hien2706/hien_data/lop_DE/log_content
#/home/hien2706/hien_data/lop_DE/result_class_3/result

