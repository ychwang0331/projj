from pyspark.sql.functions import regexp_extract, col
import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import to_date, col, count, when, isnan, regexp_replace
from pyspark.ml.feature import Imputer
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf, SQLContext
import os

# Parameters: y
# y, number of clubs that will be selected
# Return: Return list of clubs which have the most number of players with contracts ending in 2021
# y is not an integer larger than 0: return "Invalid"
# Type error: "Failed to read the data"
# Calculation error: "Something wrong in Calculation"
# Description: The function select top y clubs which have the most number of players with contracts 
# ending in 2021.

def task2_2(y):
    if isinstance(y,int)==False:
        return "Invalid"
    if y<=0:
        return "Invalid"
    try:
        appName = "Big Data Analytics"
        master = "local"
        # Create Configuration object for Spark.
        conf = pyspark.SparkConf()\
            .set('spark.driver.host','127.0.0.1')\
            .setAppName(appName)\
            .setMaster(master)\
            .set("spark.driver.extraJavaOptions", "-Xss4M")
        
        # Create Spark Context with the new configurations rather than rely on the default one
        sc = SparkContext.getOrCreate(conf=conf)

        # You need to create SQL Context to conduct some database operations like what we will see later.
        sqlContext = SQLContext(sc)

        # If you have SQL context, you create the session from the Spark Context
        spark = sqlContext.sparkSession.builder.config('spark.sql.codegen.wholeStage', 'false').getOrCreate()

        db_properties={}
        db_properties['username']="postgres"
        db_properties['password']="990331"
        # make sure to use the correct port number. These 
        db_properties['url']= "jdbc:postgresql://localhost:5432/postgres"
        db_properties['driver']="org.postgresql.Driver"
        full_imputed_df_read = sqlContext.read.format("jdbc")\
        .option("url", "jdbc:postgresql://localhost:5432/postgres")\
        .option("dbtable", "fifa.player_info")\
        .option("user", "postgres")\
        .option("password", "990331")\
        .option("Driver", "org.postgresql.Driver")\
        .load()
    except TypeError:
        print("Failed to read the data")
    try:
        df = full_imputed_df_read.filter(col("contract_valid_until") == 2021)
        df = df.groupBy("club").count()
        df = df.sort(col("count").desc())

        collect = df.collect()
        club = [""] * y
        for i in range(y):
            club[i] = collect[i][0]
    except ValueError:
        print("Something wrong in Calculation")
    return club
task2_2(5)
