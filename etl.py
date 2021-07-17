import os
import configparser
from pyspark.sql import SparkSession


def create_session(s3, sas):
    try:
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
            print("         Configurations set for SAS7DBAT and AWS")
        else:
            spark = SparkSession.builder\
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
                .enableHiveSupport().getOrCreate()
            print("         Configurations set for AWS")
                
        
        print("Spark session created!")

    except:
        print("Spark session failed.")

    return spark
    




#### ETL DIMENSION DATASET - AIRPORT ####

def etl_dim_airport(spark, file_airports):

    try:

        print("      1) Airport Codes Dimension Table ETL started...\n")

        # Load raw Airport Code data
        df = spark.read.format("csv").option("header", "true").load(file_airports)

        # Transform raw Airport Code dataset
        df.createOrReplaceTempView("df_airport")
        df_airport = spark.sql('''
                SELECT
                    iata_code as i94port,
                    municipality as city
                FROM
                    df_airport
                WHERE
                    iata_code IS NOT NULL OR iata_code != ""
        ''')

        print("Airport Codes ETL complete!\n")

    except:
        print("Airport Codes ETL failed.\n")
        
    return df_airport
        





#### ETL DIMENSION DATASET - COUNTRIES ####

def etl_dim_country(spark, file_countries):

    try:

        print("      2) Country Codes Dimension Table ETL started...\n")

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

        print("Country Codes ETL complete!\n")

    except:
        print("Country Codes ETL failed.\n")

    return df_country





#### ETL DIMENSION DATASET - STATES ####

def etl_dim_states(spark, file_states):

    try:

        print("      3) State Codes Dimension Table ETL started...\n")

        # Load raw State Codes data
        df = spark.read.format("csv").option("header", "true").load(file_states)

        # Transform raw State Codes dataset
        df.createOrReplaceTempView("df_states")
        df_states = spark.sql('''
                SELECT
                    df_states.state as i94addr,
                    df_states.state_name as state
                FROM
                    df_states
        ''')

        print("State Codes ETL complete!\n")

    except:
        print("State Codes ETL failed.\n")

    return df_states






#### ETL FACT DATASET - IMMIGRATION I94 ####

def etl_fact(spark, input_data, sas):

    if sas:
        df = spark.read.format('com.github.saurfang.sas.spark').load(input_data)
    else:
        df = spark.read.parquet(input_data)

    try:
        print("\n         Immigration Data ETL starting...\n")

        # Insert input data into a temporary view so it can be queried
        df.createOrReplaceTempView("df_immigration")
        
        # Query the specific columns and rows from the input data
        df_immigration = spark.sql('''
                SELECT
                    df_immigration.i94cit,
                    df_immigration.i94addr,
                    df_immigration.i94port,
                    df_immigration.i94yr as year,
                    df_immigration.i94mon as month,
                    df_immigration.airline as flight_airline,
                    df_immigration.FLTNO as flight_number,
                    df_immigration.i94visa as visa_code,
                    df_immigration.i94mode as mode_transport,
                    df_immigration.biryear as birth_year,
                    df_immigration.i94bir as birth_age,
                    df_immigration.gender,
                    COUNT(DISTINCT df_immigration.arrdate) as total_flights,
                    SUM(df_immigration.count) AS total_people
                FROM
                    df_immigration
                GROUP BY
                    df_immigration.i94cit,
                    df_immigration.i94addr,
                    df_immigration.i94port,
                    df_immigration.i94yr,
                    df_immigration.i94mon,
                    df_immigration.airline,
                    df_immigration.FLTNO,
                    df_immigration.i94visa,
                    df_immigration.i94mode,
                    df_immigration.biryear,
                    df_immigration.i94bir,
                    df_immigration.gender
                ''')

        print("\nImmigration Data ETL complete!\n")

    except:
        print("\nImmigration Data ETL failed!\n")
    
    return df_immigration





def clean_fact(spark, df_immigration, df_country, df_states, df_airport, output_data):

    try:

        # Insert input data into a temporary view so it can be queried
        df_immigration.createOrReplaceTempView("df_immigration")
        df_country.createOrReplaceTempView("df_country")
        df_states.createOrReplaceTempView("df_states")
        df_airport.createOrReplaceTempView("df_airport")
        

        # Add calculated columns and slice dataset for non-null records based on key dimensions
        df = spark.sql('''
                SELECT
                    df_immigration.year || df_immigration.month || df_immigration.i94port || df_immigration.flight_airline || df_immigration.flight_number || df_immigration.visa_code || df_immigration.mode_transport || df_immigration.birth_year || df_immigration.gender as irid,
                    IF(
                        df_immigration.visa_code = 1,
                        "Business",
                        IF(
                            df_immigration.visa_code = 2,
                            "Pleasure",
                            IF(
                                df_immigration.visa_code = 3,
                                "Student",
                                ""
                            )
                        )
                    ) as visa_type,
                    (df_immigration.year - df_immigration.birth_year) as age,
                    df_immigration.*
                FROM
                    df_immigration
                WHERE
                    df_immigration.i94cit IS NOT NULL AND
                    df_immigration.i94addr IS NOT NULL AND
                    df_immigration.i94port IS NOT NULL AND
                    df_immigration.flight_airline IS NOT NULL AND
                    df_immigration.birth_year IS NOT NULL AND
                    df_immigration.gender IS NOT NULL
                ''')

        df.createOrReplaceTempView("df_immigration")
        print("\n         Data slicing and calculated fields done")



        # Pull origin country name based on country code
        df = spark.sql('''
                SELECT
                    df_country.country as origin_country,
                    df_immigration.*
                FROM
                    df_immigration
                        INNER JOIN df_country
                        USING (i94cit)
                ''')

        df.createOrReplaceTempView("df_immigration")
        print("\n         Data join with country lookup done")
        

        # Pull state name based on state code
        df = spark.sql('''
                SELECT
                    df_states.state,
                    df_immigration.*
                FROM
                    df_immigration
                        INNER JOIN df_states
                        USING (i94addr)
                ''')

        df.createOrReplaceTempView("df_immigration")
        print("\n         Data join with state lookup done")


        # Pull city name based on airport
        df = spark.sql('''
                SELECT
                    df_airport.city,
                    df_immigration.*
                FROM
                    df_immigration
                        INNER JOIN df_airport
                        USING (i94port)
                ''')

        df.createOrReplaceTempView("df_immigration")
        print("\n         Data join with airport lookup done")


        print("\n         Writing final results as parquet...\n")
        df.write.parquet(output_data+"fact/",mode='overwrite',partitionBy=['year', 'month', 'i94port'])

        print("\nImmigration Data Cleaning complete!\n")
        

    except:
        print("\nImmigration Data Cleaning failed!\n")

    return df
    




def validation_fact(spark, df):

    try:

        # Insert input data into a temporary view so it can be queried
        df.createOrReplaceTempView("df_immigration")

        # Initial validation to validate that the dataframe is not empty
        if df.count() == 0:
            Exception("Invalid dataset. Immigrations fact table is empty.")



        print("\n         1) Dimension columns validation starting...\n")

        columns = [
                    ("visa_code", 3),
                    ("mode_transport", 1),
                    ("gender", 4)
        ]

        for k, v in columns:

            print("\n         Unique Values Quality Validation for Column: {}".format(k))
            
            query = spark.sql("SELECT COUNT(DISTINCT {}) FROM df_immigration".format(k))
            result = query.collect()[0][0]
            
            if v == result:
                print("         PASSED! Unique values validation...\n         Column {} has {} unique values\n         Expected values were {}".format(k, result, v))
            else:
                print("         FAILED! Unique values validation...\n         Column {} has {} unique values\n         Expected values were {}".format(k, result, v))
                

        print("\n         2) Calculated fields validation starting...\n")
        # Check age column
        df = spark.sql('''
                SELECT
                    COUNT(df_immigration.*)
                FROM
                    df_immigration
                WHERE
                    df_immigration.age != df_immigration.birth_age
                ''')

        result = df.collect()[0][0]
        
        print("         Calculated fields validation...\n         {} records have an age that doesn't match their year of birth".format(result))



        print("\nImmigration Data Validation complete!\n")




    except:
        print("\nImmigration Data Validation failed!\n")

    return df
    




def main():

    # TODO Change below to True if reading/writing from local file in test environment
    s3 = True
    
    # TODO Add S3 URI or set to local directory
    # output_data = 's3a://udacitydend20210713/fact_data/'
    output_data = 'output/'
    
    # TODO Change to True if reading SAS7DBAT files instead of parquet files
    sas = False
    
    # TODO Add SAS7DBAT files directory or parquet files (for testing purposes)
    # input_data = '../../data/*/*.sas7bdat'
    # input_data = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    input_data = 'sas_data/*'


    print("\n\nInitialize Spark...\n")
    spark = create_session(s3, sas)

    file_airports = 'airport_lookup.csv'
    file_country = 'country_lookup.csv'
    file_state = 'state_lookup.csv'

    print("\n\nDimension tables ETL started...\n")
    df_airport = etl_dim_airport(spark, file_airports)
    df_country = etl_dim_country(spark, file_country)
    df_states = etl_dim_states(spark, file_state)
    
    print("\n\nFact table ETL started...\n")
    df_immigration = etl_fact(spark, input_data, sas)

    print("\n\nFact table cleaning started...\n")
    df_immigration = clean_fact(spark, df_immigration, df_country, df_states, df_airport, output_data)


    print(df_immigration.sort(df_immigration.total_people.desc()).show(5, truncate=True)) 


if __name__ == '__main__':
    main()