

<br>

# Udacity Data Engineering Nanodegree | Capstone Project

<br>

### *Arsalan Noorafkan*

**2021-07-16**
<br>
<br>

# **Overview**
## Background

<br>

### *These facts are for a fictional scenario made up for the purposes of this project only*

<br>

A Human Trafficking Task Force has been tasked with publishing a monthly report outlining suspicious airline activity based on models trained to detect human trafficking and abuse. The team has been provided access to the US government's database of I94 immigration forms (provided as SAS7DBAT files) that are submitted by every non-citizen that enters a US port.

<br>

## Methology

The data engineers and scients have decided to break the project into three phases.

Phase 1) Stage the data using Spark and provide clean parquet files stored locally in the team's shared folder to be used for ad hoc analysis from a Jupyter notebook. During this phase, the analysts will load one month of data at a time and conduct exploratory analysis. The results will be exported as a csv file that is used to update a public Tableau dashboard.

Phase 2) Stage the data using Airflow and schedule regular monthly updates as well as a one-time historical fetch to process data going back to 2015 and stored in an S3 bucket with roles set up for specific superusers in the analytics team. During this phase, the analysts will work with data scientists to conduct analysis on historical data and review month-over-month trends and compare with the first phase's initial findings.

Phase 3) Stage the data using Airflow and schedule regular daily updates and stored in Redshift clusters that allow the data to be more accessible to teams across the country. During this phase, the team will develop a web and mobile app that will be deployed to airport security across the country that allows them to review list of flagged flights and receive notifications are irregular behaviour.

<br>

## Hypothesis

The initial hypothesis that the team wants to explore is whether there is a relationship between specific air routes and the number of I94 submissions made for groups of children. They are mainly interested at looking at aggregated data at the month and flight number level in order to capture patterns of behaviour instead of specific outlier events.


<br>

---

<br>

# **Schema**

The database will be structured as a star schema that comprises of a fact table with multiple dimension tables. This schema creates denormalized tables that help simplify queries and enable faster aggregations even when using lots of group by, joins, having, and where clauses. It must be noted that a star schema may be less flexible when working on ad-hoc queries and many-to-many relationships are not supported. 


### **Fact Table**
* i94 - each record is a unique I94 form submitted by a traveller/immigrant
    <br> `cicid, i94yr, i94mon, i94cit, i94res, i94port, arrdate, i94mode, i94addr, depdate, i94bir, i94visa, count, dtadfile, visapost, occup, entdepa, entdepd, entdepu, matflag, biryear, dtaddto, gender, insnum, airline, admnum, fltno, visatype`


### **Dimension Tables**
* country
    <br> `country_code, country_name`
* state
    <br> `state_code, state_name`
* airport
    <br> `iata_code, city, coordinates`







<br>
<br>
<br>


# **Instructions**
    

<br>

## Project Structure
- config.cfg: AWS Access Key ID and Secret Access Key
- etl.py: raw data etl and cleaning and validation
- notebook.ipynb: load clean data and run queries

<br> 

## Testing Script
Follow the steps below to test the ETL process using sample JSON data files.

The ETL process comprises of the following steps:

1) Install Python libraries
    >  `pip install pyspark`

2) Create AWS resources
    - S3 bucket us-east-1
    - Enter AWS config and authentication information in the config.cfg file

2) Open a terminal window and cd to the folder that contains the etl.py file
    > cd c:/usr/documents/Project1

3) Run the ETL process and stage the data for analysis by executing the following from the command line:
    >  `python etl.py`

4) Open notebook.ipynb and run the cells to view the results of the sample query


<br>
REMINDER: Do not include your AWS access keys in your code when sharing this project!
<br>

---


<br>

