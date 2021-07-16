# pip install ujson

import os
import configparser
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession

# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

print("\nImports completed!")


print("\nConfigurations set!")

def create_session(s3, sas):
    
    if s3:
        config = configparser.ConfigParser()
        config.read('config.cfg')
        os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
        
    if sas:
        spark = SparkSession.builder\
            .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
            .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .enableHiveSupport().getOrCreate()
    else:
        spark = SparkSession.builder\
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .enableHiveSupport().getOrCreate()
    
    return spark




#### DIMENSION DATASET - AIRPORT ####

def etl_dim_airport(spark, file_airports, output_data):

    # try:
    print("      1) Airport Codes Dimension Table ETL started...\n")
    # Load raw Airport Code data
    df = spark.read.format("csv").option("header", "true").load(file_airports)

    # Transform raw Airport Code dataset
    df.createOrReplaceTempView("df_airport")
    df_airport = spark.sql('''
            SELECT
                iata_code as i94port,
                municipality,
                coordinates
            FROM
                df_airport
            WHERE
                iata_code IS NOT NULL OR iata_code != ""
    ''')

    # Write cleaned Airport Codes data to S3 bucket as parquet files
    df_airport.write.parquet(output_data+"dim/",mode='overwrite')
    print("         Airport Codes ETL complete!\n")

    return df_airport

    # except:
    #     print("         Airport Codes ETL failed.\n")
        
    # return df_airport



#### DIMENSION DATASET - DEMOGRAPHICS ####

# def etl_dim_demographics(spark, file_demographics, output_data):

#     # try:
#     print("      2) Demographics Dimension Table ETL started...\n")
#     # Load raw Demographics data
#     df_demographics = spark.read.format("csv").option("header", "true").load(file_demographics)

#     # # Transform raw Demographics dataset
#     # df.createOrReplaceTempView("df_demographics")
#     # df_demographics = spark.sql('''
#     #         SELECT
#     #             iata_code as i94port,
#     #             municipality,
#     #             coordinates
#     #         FROM
#     #             df_demographics
#     #         WHERE
#     #             iata_code IS NOT NULL OR iata_code != ""
#     # ''')

#     # Write cleaned Airport Codes data to S3 bucket as parquet files partitioned by IATA Port Code (aka i94port)
#     df_demographics.write.parquet(output_data,mode='append')

#     print("         Airport Codes ETL complete!\n")

#     return df_demographics
    
    # except:
    #     print("         Airport Codes ETL failed.\n")
        
    # return df_demographics



#### DIMENSION DATASET - COUNTRIES ####

def etl_dim_country(spark, file_countries, output_data):

    # try:
    print("      1) Country Codes Dimension Table ETL started...\n")
    # Load raw Country Codes data
    df = spark.read.format("csv").option("header", "true").load(file_countries)

    # Transform raw Country Codes dataset
    df.createOrReplaceTempView("df_country")
    df_country = spark.sql('''
            SELECT
                df_country.code as i94cit,
                df_country.country
            FROM
                df_country
            WHERE
                df_country.country NOT LIKE "INVALID%" OR
                df_country.country NOT LIKE "NoCountry%"
    ''')

    # Write cleaned Country Codes data to S3 bucket as parquet files
    df_country.write.parquet(output_data+"dim/",mode='append')
    print("         Country Codes ETL complete!\n")

    return df_country

    # except:
    #     print("         Country Codes ETL failed.\n")
        
    # return df_country





def etl_fact(spark, input_data, output_data, sas):
    if sas:
        df = spark.read.format('com.github.saurfang.sas.spark').load(input_data)
#         df = spark.read.format('com.github.saurfang.sas.spark').load(input_data)
    else:
        df = spark.read.parquet(input_data)

    # try:
    print("\n         Immigration Data ETL starting...\n")

    # Insert input data into a temporary view so it can be queried
    df.createOrReplaceTempView("df_immigration")
    
    # Query the specific columns and rows from the input data
    df_immigration = spark.sql('''
            SELECT
                df_immigration.i94yr as year,
                df_immigration.i94mon as month,
                df_immigration.i94port as port,
                df_immigration.airline as flight_airline,
                df_immigration.FLTNO as flight_number,
                IF(
                    df_immigration.i94visa = 1,
                    "Business",
                    IF(
                        df_immigration.i94visa = 2,
                        "Pleasure",
                        IF(
                            df_immigration.i94visa = 3,
                            "Student",
                            ""
                        )
                    )
                ) as visa_type,
                df_immigration.i94mode as mode_transport,
                (df_immigration.i94yr - df_immigration.biryear) as age,
                df_immigration.gender as gender,
                COUNT(df_immigration.FLTNO) as total_flights,
                SUM(df_immigration.count) AS total_people
            FROM
                df_immigration
            WHERE
                df_immigration.biryear > 1900
            GROUP BY
                df_immigration.i94yr,
                df_immigration.i94mon,
                df_immigration.i94port,
                df_immigration.airline,
                df_immigration.FLTNO,
                df_immigration.i94visa,
                df_immigration.i94mode,
                (df_immigration.i94yr - df_immigration.biryear),
                df_immigration.gender
            SORT BY
                SUM(df_immigration.count) DESC
            ''')
    
    # Write the results of the query to S3 as parquet files
    df_immigration.write.parquet(output_data+"fact/",mode='overwrite',partitionBy=['port'])

    print("\n         Immigration Data ETL complete!\n")

    # except:
    #     print("\n\n!!! Fact Data ETL Failed !!!\n\n")
    
    return df_immigration




def check_fact(spark, df_immigration):

    # try:
    print("\n         Immigration Data ETL starting...\n")

    # Insert input data into a temporary view so it can be queried
    df_immigration.createOrReplaceTempView("df_immigration")
    
    # Query the specific columns and rows from the input data
    df_immigration = spark.sql('''
            SELECT
                df_immigration.i94yr as year,
                df_immigration.i94mon as month,
                df_immigration.i94port as port,
                df_immigration.airline as flight_airline,
                df_immigration.FLTNO as flight_number,
                df_immigration.i94visa as visa_code,
                df_immigration.i94mode as mode_transport,
                df_immigration.biryear as birth_year,
                df_immigration.gender as gender,
                SUM(df_immigration.count) AS total_migration
            FROM
                df_immigration
            GROUP BY
                df_immigration.i94yr,
                df_immigration.i94mon,
                df_immigration.i94port,
                df_immigration.airline,
                df_immigration.FLTNO,
                df_immigration.i94visa,
                df_immigration.visatype,
                df_immigration.i94mode,
                df_immigration.biryear,
                df_immigration.gender
            SORT BY
                SUM(df_immigration.count) DESC
            ''')
    
    # Write the results of the query to S3 as parquet files
    df_immigration.write.parquet(output_data+"fact/",mode='overwrite',partitionBy=['port'])

    print("\n         Immigration Data ETL complete!\n")

    # except:
    #     print("\n\n!!! Fact Data ETL Failed !!!\n\n")
    
    return df_immigration




def main():

    # TODO Change below to True if reading/writing from local file in test environment
    s3 = True
    sas = False

    # Set Spark session
    spark = create_session(s3, sas)
    print("Spark session created!")

    output_data = 'output/'

    input_data = 'sas_data/*'
#     input_data = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    file_airports = 'airport-codes_csv.csv'
    file_country = 'country_lookup.csv'
    file_state = 'state_lookup.csv'

    df_airport = etl_dim_airport(spark, file_airports, output_data)
    df_country = etl_dim_country(spark, file_country, output_data)
    df_immigration = etl_fact(spark, input_data, output_data, sas)

    print(df_immigration.show(2, truncate=False)) 


if __name__ == '__main__':
    main()