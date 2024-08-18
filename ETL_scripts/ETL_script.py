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


#---------------------------log content transformation functions------------------------------
def find_json_file(path,start_date,end_date):
    json_files = []
    for root, dirs, files in os.walk(path):
        for file in files:
            # Check if the file is a .json and if its name is between start_date and end_date
            if (file.endswith(".json")):
                if (start_date <= int(file.split(".")[0]) <= end_date):
                    json_files.append(os.path.join(root, file))
    return json_files

def convert_to_date(path):
    Str = path.split("/")[-1].split(".")[0]
    return datetime.strptime(Str, '%Y%m%d').date()

def read_json(path):
    try:
        df = spark.read.json(path)
        return df
    except Exception as e:
        print(f"An error occurred while reading the JSON file at {path}: {e}")
        return None
    
def select_fields(df):
    df = df.select("_source.*")
    return df


def calculate_total_devices(df):
    df_total_devices = df.select("Contract","Mac")\
        .groupBy(['Contract']).agg(sf.countDistinct('Mac').alias('TotalDevices'))
    return df_total_devices

def calculate_Activeness(df):
    df = df.select('Contract','Date').groupBy('Contract').agg(
        sf.countDistinct('Date').alias('Days_Active')
    )
    df = df.withColumn(
    "Activeness",
    when(col("Days_Active").between(1, 7), "very low")
    .when(col("Days_Active").between(8, 14), "low")
    .when(col("Days_Active").between(15, 21), "moderate")
    .when(col("Days_Active").between(22, 28), "high")
    .when(col("Days_Active").between(29, 31), "very high")
    .otherwise("error")
    )

    return df.filter(df.Activeness != 'error').select('Contract','Activeness')


def transform_category(df):
    df = df.withColumn('Type', when(df.AppName=='CHANNEL', 'Truyen_hinh')
                              .when(df.AppName=='DSHD', 'Truyen_hinh')
                              .when(df.AppName=='KPLUS', 'Truyen_hinh')
                              .when(df.AppName=='VOD', 'Phim_truyen')
                              .when(df.AppName=='FIMS', 'Phim_truyen')
                              .when(df.AppName=='SPORT', 'The_thao')
                              .when(df.AppName=='RELAX', 'Giai_tri')
                              .when(df.AppName=='CHILD', 'Thieu_nhi')
                              .otherwise('error'))
    df = df.filter(df.Contract != '0' )
    df = df.filter(df.Type != 'error')
    df = df.select('Contract','Type','TotalDuration')
    return df


def calculate_statistics(df):
    #Aggregate
    df = df.groupBy(['Contract','Type'])\
        .agg(sf.sum('TotalDuration').alias('TotalDuration'))
    #Convert to pivot table    
    df = df.groupBy(['Contract']).pivot('Type').sum('TotalDuration').fillna(0)

    return df

def calculate_MostWatch(df):
    df = df.withColumn("MostWatch", 
                   when(col("Truyen_hinh") == greatest("Truyen_hinh", "Phim_truyen", "The_thao", "Giai_tri", "Thieu_nhi"), "Truyen_hinh")
                   .when(col("Phim_truyen") == greatest("Truyen_hinh", "Phim_truyen", "The_thao", "Giai_tri", "Thieu_nhi"), "Phim_truyen")
                   .when(col("The_thao") == greatest("Truyen_hinh", "Phim_truyen", "The_thao", "Giai_tri", "Thieu_nhi"), "The_thao")
                   .when(col("Giai_tri") == greatest("Truyen_hinh", "Phim_truyen", "The_thao", "Giai_tri", "Thieu_nhi"), "Giai_tri")
                   .when(col("Thieu_nhi") == greatest("Truyen_hinh", "Phim_truyen", "The_thao", "Giai_tri", "Thieu_nhi"), "Thieu_nhi")
                  )
    return df

def calculate_CustomerTaste(df):
    df = df.withColumn("CustomerTaste",
                   concat_ws("-", 
                             when(col("Truyen_hinh") != 0, "Truyen_hinh"),
                             when(col("Phim_truyen") != 0, "Phim_truyen"),
                             when(col("The_thao") != 0, "The_thao"),
                             when(col("Giai_tri") != 0, "Giai_tri"),
                             when(col("Thieu_nhi") != 0, "Thieu_nhi")
                            ))
    return df


def calculate_customer_type(df):
    print("Calculating IQR")
    df = df.withColumn("TotalDuration", col("Truyen_hinh") + col("Phim_truyen") + col("The_thao") + col("Giai_tri") + col("Thieu_nhi"))
    percentiles = df.select(
    percentile_approx(
        col("TotalDuration"),
        [0.25, 0.50, 0.75],
        100
    ).alias("percentiles")
    ).collect()[0][0]

    Q1 = percentiles[0]
    median = percentiles[1]
    Q3 = percentiles[2]
    
    # -> Calculate type of customer
    # type 1 (leaving): customer that is about to leave. Activeness: very lơw, TotalDuration < 25% IQR
    # type 2 (need attention): customer that need more attention. Activeness: lơw. TotalDuration < 50% IOR
    # type 3 (normal): normal customer. Activeness: moderate. TotalDuration < 50% IQR
    # type 4 (potential): potential customer. Activeness: moderate. TotalDuration >= 50% IQR
    # type 5 (loyal): loyal customer. Activeness: high. TotalDuration > 25% IQR
    # type 6 (VIP): VIP customer. Activeness: very high. TotalDuration > 25% IQR
    # type 0 (anomaly): anomaly customer.
    
    print("Calculating CustomerType column")
    df = df.withColumn("CustomerType",
            when((col("Activeness") == "very low") & (col("TotalDuration") < Q1), 'leaving')
            .when((col("Activeness") == "low") & (col("TotalDuration") < median), 'need attention')
            .when((col("Activeness") == "moderate") & (col("TotalDuration") < median), 'normal')
            .when((col("Activeness") == "moderate") & (col("TotalDuration") >= median), 'potential')
            .when((col("Activeness") == "high") & (col("TotalDuration") > Q1), 'loyal')
            .when((col("Activeness") == "very high") & (col("TotalDuration") > Q1), 'VIP')
            .otherwise('anomaly')
        )
        
    return df.select("Contract","Giai_tri","Phim_truyen","The_thao","Thieu_nhi","Truyen_hinh","TotalDevices","MostWatch","CustomerTaste","Activeness","CustomerType")

    
#---------------------------log search transformation functions------------------------------

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
    

#----------------------------.-----------------------------  

def save_data(df_result, path_to_save):
    df_result.repartition(1).write.csv(path_to_save,
                                       #mode = 'overwrite',
                                       header = True)
    return print(f"Data saved successfully at {path_to_save}")

def write_to_MySQL(df,host,port,database_name,table,user,password):
    df.write.format("jdbc").options(
        url=f"jdbc:mysql://{host}:{port}/{database_name}",
        driver = "com.mysql.cj.jdbc.Driver",
        dbtable = table,
        user= user,
        password= password).mode('overwrite').save()   
    

def main(log_content_path,
         log_search_path,
         path_to_save,
         path_of_mapping_file,
         log_content_start_date,
         log_content_end_date,
         log_search_start_date,
         log_search_end_date):
    print('------------------Processing log content------------------')
    
    print('Finding json files from path')
    json_files = find_json_file(log_content_path,log_content_start_date,log_content_end_date)
    if not json_files:
        return print(f"Not detecting any .json files from {log_content_path} with date from {log_content_start_date} to {log_content_end_date}")
    
    print('Reading and Unionizing JSON files')
    df_union_log_content = None
    for json_file in json_files:
        df = read_json(json_file)
        if df is not None:
            #Select fields and add Date
            df = select_fields(df).withColumn('Date', lit(convert_to_date(json_file)))
            if df_union_log_content is None:
                df_union_log_content = df
            else:
                df_union_log_content = df_union_log_content.unionByName(df)
                df_union_log_content = df_union_log_content.cache()
                
    if df_union_log_content is None:
        return print('No DataFrames created from JSON files.')
    
    print('Calculating TotalDevices column')
    df_total_devices = calculate_total_devices(df_union_log_content)
    
    print('Calculating Activeness column')
    df_activeness = calculate_Activeness(df_union_log_content)
    
    print('Transforming Category')
    df_union_log_content = transform_category(df_union_log_content)
    
    print('Calculating Statistics')
    df_union_log_content = calculate_statistics(df_union_log_content)
    
    print('Calculating MostWatch column')
    df_union_log_content = calculate_MostWatch(df_union_log_content)
    
    print('Calculating CustomerTaste column')
    df_union_log_content = calculate_CustomerTaste(df_union_log_content)
    
    print('Adding Activeness and TotalDevices column')
    df_union_log_content = df_union_log_content.join(df_activeness, on = ['Contract'], how = 'inner')
    df_union_log_content = df_union_log_content.join(df_total_devices, on = ['Contract'], how = 'inner')
    
    print('Calculating CustomerType column')
    df_union_log_content = calculate_customer_type(df_union_log_content)
    
    print("Rename columns")
    rename_column = ["Giai_tri","Phim_truyen","The_thao","Thieu_nhi","Truyen_hinh"]
    for old_name in rename_column:
        df_union_log_content = df_union_log_content.withColumnRenamed(old_name,"Total_"+old_name)
    
    print("Result of log content after transformation:")
    df_union_log_content.show(30,truncate = False)
    print(df_union_log_content)
    
    print('------------------Processing log search------------------')
    
    print('Reading parquet files from path')
    parquet_files = os.listdir(log_search_path)
    parquet_files = [os.path.join(log_search_path , file) for file in parquet_files if log_search_start_date <= int(file) <= log_search_end_date]
    
    if not parquet_files:
        return print(f"No parquet files found in the path {log_search_path}")
    
    print('-------------Reading and Unionizing parquet files--------------')
    df_union_log_search = None
    for file in parquet_files:
        df = spark.read.parquet(file)
        df = df.select(month(to_date(df.datetime)).alias("month"), "user_id", "keyword")
        if df is not None:
            if df_union_log_search is None:
                df_union_log_search = df
            else:
                df_union_log_search = df_union_log_search.unionByName(df)
                df_union_log_search = df_union_log_search.cache()
                
    if df_union_log_search is None:
        return print('No DataFrames created from parquet files.')
    
    print('Filtering out rows with null values, duplicates, and columns not in month 6 and 7')
    
    df_union_log_search = filter_null_duplicates_and_month(df_union_log_search)
    
    print('Find most searched keyword for each user')
    df_union_log_search = find_most_searched_keyword(df_union_log_search)
    
    print('Find most searched keyword for each user in month 6 and 7') 
    df_union_log_search = get_most_searched_keywords_trimmed(df_union_log_search, 6, 7)
    
    print('Reading mapping file')
    mapping_df = spark.read.csv(path_of_mapping_file, header=True).dropDuplicates(["search"])
    
    
    print('Get search category')
    df_union_log_search = get_search_category(df_union_log_search, mapping_df)
    
    print('Get Trending Type column')
    df_union_log_search = get_Trending_type_column(df_union_log_search)
    
    print('Get Previous column')
    df_union_log_search = get_Previous_column(df_union_log_search)
    
    print('Preview of log search after transformation')
    df_union_log_search.show(30,truncate=False)
    print(df_union_log_search)    
    
    print('Unionizing log content and log search')
    df_union_log_content = df_union_log_content.limit(250)
    df_union_log_search = df_union_log_search.limit(250)
    
    df_union_log_content = df_union_log_content.withColumn("index", monotonically_increasing_id())
    df_union_log_search = df_union_log_search.withColumn("index", monotonically_increasing_id())

    df_union = df_union_log_content.join(df_union_log_search, on="index").drop("index","user_id")

    df_union.show(30,truncate=False)
    
    print('Saving data into csv file')
    save_data(df_union,path_to_save)
    
    check_flag = input('Write to mysql? (Y/n): ')
    if check_flag.lower() != 'y':
        return print('Task finished')
    
    print('-------------Loading result into MySql db--------------')
    host = input('enter hostname:')
    port = input('enter port:')
    user = input('enter user:')
    password = input('enter password:')
    database_name = input('enter database name:')
    table = input('enter table name:')
             
    write_to_MySQL(df = df_union,
                   host= host,
                   port=port,
                   database_name=database_name,
                   table=table,
                   user= user,
                   password=password)
    
    return
    
    
if __name__ == "__main__":
    
    spark = (SparkSession.builder
            .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.30")
            .config("spark.driver.memory", "6g")
            .getOrCreate())

    log_content_path = input('enter log_content path: ')
    log_search_path = input('enter log_search path: ')
    path_to_save = input('enter path to save into a csv file: ')
    path_of_mapping_file = input('enter path of mapping file: ')
    log_content_start_date = input('enter the start date of log_content files')
    log_content_end_date = input('enter the end date of log_content files')
    log_search_start_date = input('enter the start date of log_search files')
    log_search_end_date = input('enter the end date of log_search files')
    
    main(log_content_path,
         log_search_path,
         path_to_save,
         path_of_mapping_file,
         log_content_start_date,
         log_content_end_date,
         log_search_start_date,
         log_search_end_date)
