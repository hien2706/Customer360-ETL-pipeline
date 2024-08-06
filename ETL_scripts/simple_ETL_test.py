import pandas as pd
import findspark
findspark.init()
import pyspark
from pyspark.sql.functions import *
from pyspark.sql import functions as sf
from pyspark.sql import SparkSession
import os
from datetime import datetime

# add packages when running PySpark on terminal: pyspark --packages com.mysql:mysql-connector-j:8.0.33

# Execution plan: simple_ETL_v4.py
# -> union all files into a single df and add date in each section according to file's name

# column:  |AppName| Contract| Mac| TotalDuration| Date|


# -> calculate into TotalDevices df:
# + select Contract,MAC
# + group by Contract and countDistinct MAC -> column: Contract | TotalDevices

# -> calculate Activeness df:
# + select Date, Contract
# + group by Contract countDistinct(Date) -> column: Contract | Activeness
# logic:
# 1-7: very low
# 8-14: low
# 15-21: moderate
# 21-27: high
# 27-31: very high


# ->Categorize values in AppName, Ex:Truyen_hinh, The_thao -> change to new column name: Type
# + select column | Contract | Type | Duration | to reduce redundant columns(not used for further calculation)

# ->group by 'Contract','Type' and calculate sum duration for each of them 
# and convert to pivot table

# column: | Contract  | Truyen_hinh | Phim_truyen | The_thao | Giai_tri | Thieu_nhi | 

# ->calculating MostWatch column:
# the value of MostWatch column is the column's name that has the highest value in (Truyen_hinh | Phim_truyen | The_thao | Giai_tri | Thieu_nhi) 

# ->calculating CustomerTaste column:
# concatenate the column name of (Truyen_hinh | Phim_truyen | The_thao | Giai_tri | Thieu_nhi) if the value in those column <> 0 into a new column: CustomerTaste.

# -> finalize_results:
# join TotalDevices,Activeness with current df (on Contract) to get the column TotalDevices and Activeness

# -> Calculate type of customer
# type 1: customer that is about to leave. Activeness: very lơw, TotalDuration < 25% IQR
# type 2: customer that need more attention. Activeness: lơw. TotalDuration < 50% IOR
# type 3: normal customer. Activeness: moderate. TotalDuration < 50% IQR
# type 4: potential customer. Activeness: moderate. TotalDuration >= 50% IQR
# type 5: loyal customer. Activeness: high. TotalDuration > 25% IQR
# type 6: VIP customer. Activeness: very high. TotalDuration > 25% IQR
# type 0: anomaly customer.

# ->save to csv, write into mysql db 

spark = (SparkSession.builder
         .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.30")
         .config("spark.driver.memory", "6g")
         .getOrCreate())

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
    # type 1: customer that is about to leave. Activeness: very lơw, TotalDuration < 25% IQR
    # type 2: customer that need more attention. Activeness: lơw. TotalDuration < 50% IOR
    # type 3: normal customer. Activeness: moderate. TotalDuration < 50% IQR
    # type 4: potential customer. Activeness: moderate. TotalDuration >= 50% IQR
    # type 5: loyal customer. Activeness: high. TotalDuration > 25% IQR
    # type 6: VIP customer. Activeness: very high. TotalDuration > 25% IQR
    # type 0: anomaly customer.
    
    print("Calculating CustomerType column")
    df = df.withColumn("CustomerType",
            when((col("Activeness") == "very low") & (col("TotalDuration") < Q1), 1)
            .when((col("Activeness") == "low") & (col("TotalDuration") < median), 2)
            .when((col("Activeness") == "moderate") & (col("TotalDuration") < median), 3)
            .when((col("Activeness") == "moderate") & (col("TotalDuration") >= median), 4)
            .when((col("Activeness") == "high") & (col("TotalDuration") > Q1), 5)
            .when((col("Activeness") == "very high") & (col("TotalDuration") > Q1), 6)
            .otherwise(0)
        )
        
    return df.select("Contract","Giai_tri","Phim_truyen","The_thao","Thieu_nhi","Truyen_hinh","TotalDevices","MostWatch","CustomerTaste","Activeness","CustomerType")

    

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
          
def main(path_to_read,path_to_save,
         start_date,end_date):
    
    # print('-------------Validating paths--------------')
    # if not os.path.exists(path_to_read):
    #     return print(f"{path_to_read} does not exist to read from")
    # if os.path.exists(path_to_save):
    #     return print(f"{path_to_save} already exists")
    
    print('-------------Finding json files from path--------------')
    json_files = find_json_file(path_to_read,start_date,end_date)
    if not json_files:
        return print(f"Not detecting any .json files from {path_to_read} with date from {start_date} to {end_date}")
    
    print('-------------Reading and Unionizing JSON files--------------')
    df_union = None
    for json_file in json_files:
        df = read_json(json_file)
        if df is not None:
            #Select fields and add Date
            df = select_fields(df).withColumn('Date', lit(convert_to_date(json_file)))
            if df_union is None:
                df_union = df
            else:
                df_union = df_union.unionByName(df)
                df_union = df_union.cache()
                
    if df_union is None:
        return print('No DataFrames created from JSON files.')
    
    
    print('Calculating TotalDevices column')
    df_total_devices = calculate_total_devices(df_union)
    
    print('Calculating Activeness column')
    df_activeness = calculate_Activeness(df_union)
    
    print('Transforming Category')
    df_union = transform_category(df_union)
    
    print('Calculating Statistics')
    df_union = calculate_statistics(df_union)
    
    print('Calculating MostWatch column')
    df_union = calculate_MostWatch(df_union)
    
    print('Calculating CustomerTaste column')
    df_union = calculate_CustomerTaste(df_union)
    
    print('Adding Activeness and TotalDevices column')
    df_union = df_union.join(df_activeness, on = ['Contract'], how = 'inner')
    df_union = df_union.join(df_total_devices, on = ['Contract'], how = 'inner')
    
    print('Calculating CustomerType column')
    df_union = calculate_customer_type(df_union)
    
    print("Rename columns")
    rename_column = ["Giai_tri","Phim_truyen","The_thao","Thieu_nhi","Truyen_hinh"]
    for old_name in rename_column:
        df_union = df_union.withColumnRenamed(old_name,"Total_"+old_name)
    
    print("Result:")
    df_union.show(30,truncate = False)
    print(df_union)
    
    # print('-------------Saving result into csv file--------------')
    # save_data(df_union,path_to_save)
    
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
    # table = 'customer_interaction_data'
    # user = 'hien2706'
    # password = '5conmeocoN@'
    
    # RDS MySQL credentials
    host = "customer360-db.c1uy4eie6gye.ap-southeast-1.rds.amazonaws.com"
    port = 3306
    database_name = "customer_data"
    table = "customer_interaction_data"
    user = "admin"
    password = "5conmeocon"

    df_test = df_union.limit(250)
    print('-------------Loading result into MySql db--------------')
    write_to_MySQL(df = df_test,
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
    # start_date = int(input("Enter the start date to take data (YYYYMMDD): "))
    # end_date = int(input("Enter the end date to take data (YYYYMMDD): "))
    
    path_to_read = "/home/hien2706/hien_data/lop_DE/log_content"
    path_to_save = "/home/hien2706/hien_data/lop_DE/result_class_3/"
    start_date = 20220401
    end_date = 20220430
    

    main(path_to_read = path_to_read, 
        path_to_save = os.path.join(path_to_save,f"{start_date}-{end_date}"),
        start_date = start_date,
        end_date = end_date)    

#/home/hien2706/hien_data/lop_DE/log_content
#/home/hien2706/hien_data/lop_DE/result_class_3/result


