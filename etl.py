# pip install ujson

import os
import configparser
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession

# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

print("\nImports completed!")





print("\nConfigurations set!")

def create_session(local):
    
    if local:
        config = configparser.ConfigParser()
        config.read('config.cfg')
        os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:3.0.0-s_2.12") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark









#### DIMENSION DATASET - AIRPORT ####

def etl_dim_airport(spark, file_airports, output_data):

    try:
        print("      1) Airport Codes Dimension Table ETL started...\n")
        # Load raw Airport Code data
        df = spark.read.format("csv").option("header", "true").load(airport_data)

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

        # Write cleaned Airport Codes data to S3 bucket as parquet files partitioned by IATA Port Code (aka i94port)
        df_airport.write.parquet(output_data,mode='append',partitionBy=['i94port'])
        print("         Airport Codes ETL complete!\n")

        return df_airport

    except:
        print("         Airport Codes ETL failed.\n")
        
    # return df_airport



#### DIMENSION DATASET - DEMOGRAPHICS ####

def etl_dim_demographics(spark, file_demographics, output_data):

    try:
        print("      2) Demographics Dimension Table ETL started...\n")
        # Load raw Demographics data
        df = spark.read.format("csv").option("header", "true").load(file_demographics)

        # Transform raw Demographics dataset
        df.createOrReplaceTempView("df_demographics")
        df_demographics = spark.sql('''
                SELECT
                    iata_code as i94port,
                    municipality,
                    coordinates
                FROM
                    df_demographics
                WHERE
                    iata_code IS NOT NULL OR iata_code != ""
        ''')

        # Write cleaned Airport Codes data to S3 bucket as parquet files partitioned by IATA Port Code (aka i94port)
        df_demographics.write.mode("append").partitionBy("i94port").parquet(os.path.join(output_data, "/fact_data/immigration.parquet"))

        print("         Airport Codes ETL complete!\n")

        return df_demographics
    
    except:
        print("         Airport Codes ETL failed.\n")
        
    # return df_demographics



















def etl_fact(spark, input_data, output_data, df_airport, local):
    if local:
        df = spark.read.parquet(input_data)
    else:
        df = spark.read.format('com.github.saurfang.sas.spark').load(input_data)

    try:
        print("\n         Immigration Data ETL starting...\n")

        # Insert input data into a temporary view so it can be queried
        df_airport.createOrReplaceTempView("df_airport")
        df.createOrReplaceTempView("df_immigration")
        
        # Query the specific columns and rows from the input data
        df_immigration = spark.sql('''
                SELECT
                    df_immigration.i94yr as year,
                    df_immigration.i94mon as month,
                    SUBSTR(df_immigration.i94cit, 1, INSTR(df_immigration.i94cit, ",")) as city,
                    df_immigration.i94port as port,
                    df_immigration.arrdate as arrival_date,
                    df_immigration.i94visa as reason,
                    df_airport.municipality as city,
                    df_airport.coordinates as coordinates
                FROM
                    df_immigration
                        JOIN df_airport USING (i94port)
                WHERE
                    df_immigration.i94port IS NOT NULL OR df_immigration.i94port != ""
                ''')
        
        # Write the results of the query to S3 as parquet files
        df_immigration.write.parquet(output_data,mode='append',partitionBy=['port'])

        print("\n         Immigration Data ETL complete!\n")

    except:
        print("\n\n!!! Fact Data ETL Failed !!!\n\n")





def main():

    # TODO Change below to True if reading/writing from local file in test environment
    local = True

    # Set Spark session
    spark = create_session(local)
    print("Spark session created!")


#     output_data = 's3a://udacitydend20210713/fact_data/'
    output_data = 'output/'

    input_data = 'sas_data/part-00000-b9542815-7a8d-45fc-9c67-c9c5007ad0d4-c000.snappy.parquet'
    file_airports = 'airport-codes_csv.csv'
    file_demographics = 'us-cities-demographics.csv'

    df_airport = etl_dim_airport(spark, file_airports, output_data)
    df_demographics = etl_dim_demographics(spark, file_demographics, output_data)
    etl_fact(spark, input_data, output_data, df_airport, local)

if __name__ == '__main__':
    main()