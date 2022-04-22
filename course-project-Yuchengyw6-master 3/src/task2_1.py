from pyspark.sql.functions import regexp_extract, col
import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import to_date, col, count, when, isnan, regexp_replace
from pyspark.ml.feature import Imputer
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf, SQLContext
import os
# Parameters: year1, year2, x
# year1, year2, the year used to select the players who improve the most. year2 is the more recent year
# x, number of players that will be selected
# Return: Return list of players who improve the most if the input values are valid. Else return 
# corresponding error message.
# year1 not in 2015-2020: return "Invalid"
# year2 not in 2015-2020: return "Invalid"
# year1 is later than year2: return "Invalid"
# x is not an integer larger than 0: return "Invalid"
# Reading type error: return "Failed to read the data"
# Calculation error: return "Something wrong in Calculation"
# Description: The function select top x players who improve the most from year1 to year2. 

def task2_1(year1,year2,x):
    if year1 not in [2015,2016,2017,2018,2019,2020]:
        return "Invalid"
    if year2 not in [2015,2016,2017,2018,2019,2020]:
        return "Invalid"
    if year1>year2:
        return "Invalid"
    if isinstance(x,int)==False:
        return "Invalid"
    if x<=0:
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
        df1 = full_imputed_df_read.where(full_imputed_df_read.year==year1)
        df2 = full_imputed_df_read.where(full_imputed_df_read.year==year2)
        df_2015_imputed_read = df1.withColumn("improve1",(col("skill_moves")+
                                               col("skill_dribbling")+
                                               col("skill_curve")+
                                               col("skill_fk_accuracy")+
                                               col("skill_long_passing")+
                                               col("skill_ball_control")))
        df_2015_imputed_read = df_2015_imputed_read.select(("long_name"),("improve1"))

        df_2020_imputed_read = df2.withColumn("improve2",(col("skill_moves")+
                                               col("skill_dribbling")+
                                               col("skill_curve")+
                                               col("skill_fk_accuracy")+
                                               col("skill_long_passing")+
                                               col("skill_ball_control")))
        df_2020_imputed_read = df_2020_imputed_read.select(("long_name"),("improve2"))

        df = df_2015_imputed_read.join(df_2020_imputed_read,
                                       df_2015_imputed_read.long_name == df_2020_imputed_read.long_name,
                                       "inner")
        df = df.withColumn("improve",(col("improve2")-col("improve1")))
        df = df.sort(col("improve").desc())
        collect = df.collect()

        l = [None] * x
        name = [""] * x
        for i in range(x):
            l[i] = collect[i][-1]
            name[i] = collect[i][0]
    except ValueError:
        print("Something wrong in Calculation")
    return name 
task2_1(2015,2020,5)
