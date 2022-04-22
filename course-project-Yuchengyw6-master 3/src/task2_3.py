from pyspark.sql.functions import regexp_extract, col
import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import to_date, col, count, when, isnan, regexp_replace
from pyspark.ml.feature import Imputer
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf, SQLContext
import os

# Parameters: z, year
# z, number of clubs that will be selected
# year, value of year that will be considered
# Return: return list of clubs with largest number of players in the dataset where z >= 5.
# z is not an integer: return "Invalid"
# z is less than 5: return "z is not big enough"
# Type error: "Failed to read the data"
# Calculation error: "Something wrong in Calculation"
# Description: The function select top z clubs with largest number of players in the dataset where 
# z >= 5.
def task2_3(z,year="all"):
    if isinstance(z,int)==False:
        return "Invalid"
    if z < 5:
        print("z is not big enough")
        return "z is not big enough"
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
        if year == "all":
            full_imputed_df_read = full_imputed_df_read
        elif year in [2015,2016,2017,2018,2019,2020]:
            full_imputed_df_read = full_imputed_df_read.filter(col("year") == year)
        else: 
            return "invalid input"
    except TypeError:
        print("Failed to read the data")
    try:
        df = full_imputed_df_read.groupBy("club").count()
        df = df.sort(col("count").desc())

        is_false = True

        collect = df.collect()
        club = [""] * z
        club[0] = collect[0][0]
        count = [None] * z
        count[0] = collect[0][1]
        for i in range(1,z):
            club[i] = collect[i][0]
            count[i] = collect[i][1]
            if count[i-1] != count[i]:
                is_false = False
    except ValueError:
        print("Something wrong in Calculation")
    
    if is_false == True:
        return "All the same"
    else:
        return club
task2_3(5)
