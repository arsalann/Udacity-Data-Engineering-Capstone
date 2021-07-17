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

def etl_dim_airport(spark, input_data, output_data):
    '''
    Function: 
        1) loads input dimension table: lookup_airport
        2) create a temporary spark view: df_airport
        3) stage the data using a spark sql query
        4) write to storage as parquet files
    Parameters:
        spark: the spark session cursor
        input_data: csv file
        output_path: path to S3 bucket or local directory
    Returns:
        Dataframe
    '''

    try:

        print("      1) Airport Codes Dimension Table ETL started...\n")

        # Load raw Airport Code data
        df = spark.read.format("csv").option("header", "true").load(input_data)

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
    
    df_airport.write.parquet(output_data+"dim/",mode='overwrite')

    return df_airport
        





#### ETL DIMENSION DATASET - COUNTRIES ####

def etl_dim_country(spark, input_data, output_data):
    '''
    Function: 
        1) loads input dimension table: lookup_country
        2) create a temporary spark view: df_country
        3) stage the data using a spark sql query
        4) write to storage as parquet files
    Parameters:
        spark: the spark session cursor
        input_data: csv file
        output_path: path to S3 bucket or local directory
    Returns:
        Dataframe
    '''

    try:

        print("      2) Country Codes Dimension Table ETL started...\n")

        # Load raw Country Codes data
        df = spark.read.format("csv").option("header", "true").load(input_data)

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

    df_country.write.parquet(output_data+"dim/",mode='overwrite')
    
    return df_country





#### ETL DIMENSION DATASET - STATES ####

def etl_dim_states(spark, input_data, output_data):
    '''
    Function: 
        1) loads input dimension table: lookup_state
        2) create a temporary spark view: df_state
        3) stage the data using a spark sql query
        4) write to storage as parquet files
    Parameters:
        spark: the spark session cursor
        input_data: csv file
        output_path: path to S3 bucket or local directory
    Returns:
        Dataframe
    '''

    try:

        print("      3) State Codes Dimension Table ETL started...\n")

        # Load raw State Codes data
        df = spark.read.format("csv").option("header", "true").load(input_data)

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

    print("\n         Writing final results as parquet...\n")
    df_states.write.parquet(output_data+"dim/",mode='overwrite')

    return df_states






#### ETL FACT DATASET - IMMIGRATION I94 ####

def etl_fact(spark, input_data, output_data, sas):
    '''
    Function: 
        1) loads raw fact data
        2) create a temporary spark view: df_immigration
        3) stage the data using a spark sql query
        4) write to storage as parquet files
    Parameters:
        spark: the spark session cursor
        input_data: sas7dbat or parquet files
        output_path: path to S3 bucket or local directory
        sas: user entered parameter to switch sas7dbat and parquet format when reading input data 
            if sas is true, it will read source sas7dbat files
            otherwise, it will read from local parquet files (test environment)
    Returns:
        Dataframe
    '''

    if sas:
        df = spark.read.format('com.github.saurfang.sas.spark').load(input_data)
    else:
        df = spark.read.parquet(input_data)

    # Initial validation to validate that the dataframe is not empty
    if df.count() == 0:
        Exception("Invalid dataset. Immigrations fact table is empty.")
    else:
        print("Total Records Loaded: " + str(df.count()))
        
    try:
        print("\n         Immigration Data ETL starting...\n")

        # Insert input data into a temporary view so it can be queried
        df.createOrReplaceTempView("df_immigration")
        
        # Query the specific columns and rows from the input data
        df_immigration = spark.sql('''
                SELECT
                    df_immigration.i94cit,
                    df_immigration.i94res,
                    df_immigration.i94addr,
                    df_immigration.i94port,
                    df_immigration.i94yr as year,
                    df_immigration.i94mon as month,
                    df_immigration.airline as flight_airline,
                    df_immigration.FLTNO as flight_number,
                    df_immigration.i94visa as visa_code,
                    df_immigration.visatype as visatype,
                    df_immigration.i94mode as mode_code,
                    df_immigration.biryear as birth_year,
                    df_immigration.i94bir as birth_age,
                    df_immigration.gender,
                    COUNT(DISTINCT df_immigration.arrdate) as total_flights,
                    SUM(df_immigration.count) AS total_people
                FROM
                    df_immigration
                WHERE
                    df_immigration.i94mode = 1
                GROUP BY
                    df_immigration.i94cit,
                    df_immigration.i94res,
                    df_immigration.i94addr,
                    df_immigration.i94port,
                    df_immigration.i94yr,
                    df_immigration.i94mon,
                    df_immigration.airline,
                    df_immigration.FLTNO,
                    df_immigration.i94visa,
                    df_immigration.visatype,
                    df_immigration.i94mode,
                    df_immigration.biryear,
                    df_immigration.i94bir,
                    df_immigration.gender
                ''')

        print("\nImmigration Data ETL complete!\n")

        print("\n         Writing final results as parquet...\n")
        df_immigration.write.parquet(output_data+"fact/etl/",mode='overwrite',partitionBy=['year', 'month', 'i94addr'])

    except:
        print("\nImmigration Data ETL failed!\n")
    
    return df_immigration




#### CLEAN FACT DATASET ####

def clean_fact(spark, df_immigration, df_country, df_states, df_airport, output_data):
    '''
    Function: 
        1) loads the results from etl_fact
        2) loads the results from dimension table etl functions
        3) create a temporary spark view for all the input dataframes
        4) stage the data using spark sql queries
        5) write to storage as parquet files
    Parameters:
        spark: the spark session cursor
        df_immigration: dataframe returned by etl_fact
        df_country: dataframe returned by etl_dim_country
        df_states: dataframe returned by etl_dim_states
        df_airport: dataframe returned by etl_dim_airport
        output_path: path to S3 bucket or local directory
    Returns:
        Dataframe
    '''

    try:

        # Insert input data into a temporary view so it can be queried
        df_immigration.createOrReplaceTempView("df_immigration")
        df_country.createOrReplaceTempView("df_country")
        df_states.createOrReplaceTempView("df_states")
        df_airport.createOrReplaceTempView("df_airport")
        

        # Add calculated columns and slice dataset for non-null records based on key dimensions
        df = spark.sql('''
                SELECT
                    df_immigration.year || df_immigration.i94cit || df_immigration.month || df_immigration.i94port || df_immigration.flight_airline || df_immigration.flight_number || df_immigration.visa_code || df_immigration.visatype || df_immigration.birth_year || df_immigration.gender as irid,
                    df_immigration.year,
                    df_immigration.month,
                    df_immigration.i94addr,
                    df_immigration.i94port,
                    df_immigration.flight_airline,
                    df_immigration.flight_number,
                    df_immigration.gender,
                    df_immigration.i94cit,
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
                    df_immigration.birth_year,
                    df_immigration.birth_age,
                    df_immigration.total_flights,
                    df_immigration.total_people

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
        df.write.parquet(output_data+"fact/clean/",mode='overwrite',partitionBy=['year', 'month', 'state'])

        print("\nImmigration Data Cleaning complete!\n")
        

    except:
        print("\nImmigration Data Cleaning failed!\n")

    return df
    




#### VALIDATE FACT DATASET ####

def validation(spark, df):
    '''
    Function: 
        1) loads the results from clean_fact
        2) create a temporary spark view
        3) check if the input dataframe is empty
        4) loop through key dimension columns and validate count of unique values
        5) quality check the irid and determine if any duplicate records exist
    Parameters:
        spark: the spark session cursor
        df: dataframe returned by clean_fact
    Returns:
        None
    '''

    # try:

    # Insert input data into a temporary view so it can be queried
    df.createOrReplaceTempView("df_immigration")

    # Initial validation to validate that the dataframe is not empty
    if df.count() == 0:
        Exception("Invalid dataset. Immigrations fact table is empty.")
    else:
        print("Total Records Loaded: " + str(df.count()))



    print("\n         1) Dimension columns validation starting...\n")

    columns = [
                ("visa_type", 3),
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

    # Check IRID for duplicates
    query = spark.sql("SELECT COUNT(irid) FROM df_immigration GROUP BY irid ORDER BY COUNT(irid) DESC".format(k))
    result = query.collect()[0][0]

    if result == 1:
        print("         PASSED! Internal Reference ID validation...\n         No duplicate IRIDs found.")
    else:
        print("         FAILED! Internal Reference ID validation...\n         Max number of duplicate IRIDs = {}".format(result))


    print("\nImmigration Data Validation complete!\n")


    # except:
    #     print("\nImmigration Data Validation failed!\n")

    return df
    


def queries(spark, df):
    '''
    Function: 
        1) loads the results from clean_fact
        2) create a temporary spark view
        3) execute sample queries
    Parameters:
        spark: the spark session cursor
        df: dataframe returned by clean_fact
    Returns:
        None
    '''

    try:

        # Insert input data into a temporary view so it can be queried
        df.createOrReplaceTempView("df")

        print("Query 1: Top 5 routes with the most number of teenagers entered in a given month")
        query = spark.sql("""
            SELECT
                df.month,
                df.state,
                df.city,
                df.origin_country,
                df.gender,
                df.age,
                SUM(df.total_flights) AS total_flights,
                SUM(df.total_people) AS total_people,
                SUM(df.total_people)/SUM(df.total_flights) AS avg_per_flight
            FROM
                df
            WHERE
                df.month = 4 AND
                df.age < 19 AND
                df.age > 12
            GROUP BY
                df.month,
                df.state,
                df.city,
                df.origin_country,
                df.gender,
                df.age
            ORDER BY
                SUM(df.total_people) DESC
        """)

        print(query.sort(query.total_people.desc()).show(5, truncate=True))

        
        print("Query 2: Routes with more than 5 teenagers per flight")
        query.createOrReplaceTempView("query1")

        query = spark.sql("""
            SELECT
                query1.*
            FROM
                query1
            WHERE
                query1.avg_per_flight > 5
        """)

        print(query.sort(query.total_people.desc()).show(5, truncate=True))
        query.createOrReplaceTempView("query2")


        print("\nSample queries complete!\n")


    except:
        print("\nSample queries failed!\n")

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
    # input_data = 'sas_data/*'
    input_data = 'sas_data/part-00002-b9542815-7a8d-45fc-9c67-c9c5007ad0d4-c000.snappy.parquet'


    print("\n\nInitialize Spark...\n")
    spark = create_session(s3, sas)

    file_airports = 'airport_lookup.csv'
    file_country = 'country_lookup.csv'
    file_state = 'state_lookup.csv'

    print("\n\nDimension tables ETL started...\n")
    df_airport = etl_dim_airport(spark, file_airports, output_data)
    df_country = etl_dim_country(spark, file_country, output_data)
    df_states = etl_dim_states(spark, file_state, output_data)
    
    print("\n\nFact table ETL started...\n")
    df_immigration = etl_fact(spark, input_data, output_data, sas)

    print("\n\nFact table cleaning started...\n")
    df_immigration = clean_fact(spark, df_immigration, df_country, df_states, df_airport, output_data)

    print(df_immigration.sort(df_immigration.total_people.desc()).show(5, truncate=False)) 

    print("\n\nValidation started...\n")
    validation(spark, df_immigration)

    print("\n\nSample queries started...\n")
    queries(spark, df_immigration)

    print("\n\n\n DATA PIPELINE COMPLETE")


if __name__ == '__main__':
    main()