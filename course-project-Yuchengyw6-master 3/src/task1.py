from pyspark.sql.functions import regexp_extract, col
import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import to_date, col, count, when, isnan, regexp_replace
from pyspark.ml.feature import Imputer
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf, SQLContext
import os
# PARAMETER: NONE
# RETURN: Combination of databases from 2015 to 2020
# DESCRIPTION: Ingest FIFA player data from 2015 to 2020, drop columns with number of missing 
# values more than half of players, process the columns with strings containing character "+" and 
# "-" by converting the formulas to numeric values, impute the missing values by mean values, and 
# save the databases from 2015 to 2020 and the combined database into PostgreSQL.
def task1():
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

        #Ingest data from the players.csv into Spark Dataframe. 
        players_15_df = (spark.read
                 .format("csv")
                 .option("inferSchema", "true")
                 .option("header","true")
                 .load("/Users/wyc/players_15.csv")
              )
        players_15_df = players_15_df.withColumn("year",lit(2015))

        players_16_df = (spark.read
                 .format("csv")
                 .option("inferSchema", "true")
                 .option("header","true")
                 .load("/Users/wyc/players_16.csv")
              )
        players_16_df = players_16_df.withColumn("year",lit(2016))

        players_17_df = (spark.read
                 .format("csv")
                 .option("inferSchema", "true")
                 .option("header","true")
                 .load("/Users/wyc/players_17.csv")
              )
        players_17_df = players_17_df.withColumn("year",lit(2017))

        players_18_df = (spark.read
                 .format("csv")
                 .option("inferSchema", "true")
                 .option("header","true")
                 .load("/Users/wyc/players_18.csv")
              )
        players_18_df = players_18_df.withColumn("year",lit(2018))

        players_19_df = (spark.read
                 .format("csv")
                 .option("inferSchema", "true")
                 .option("header","true")
                 .load("/Users/wyc/players_19.csv")
              )
        players_19_df = players_19_df.withColumn("year",lit(2019))

        players_20_df = (spark.read
                 .format("csv")
                 .option("inferSchema", "true")
                 .option("header","true")
                 .load("/Users/wyc/players_20.csv")
              )
        players_20_df = players_20_df.withColumn("year",lit(2020))
    except TypeError:
        print("Failed to load the data")
    merged_df = players_15_df.union(players_16_df).union(players_17_df)\
            .union(players_18_df).union(players_19_df).union(players_20_df)
    full_df = merged_df
    total_rows = full_df.count()
    th = total_rows / 2
    variables = ['sofifa_id','player_url','short_name','long_name','age','dob','height_cm','weight_kg','nationality','club','overall','potential','value_eur','wage_eur','player_positions','preferred_foot','international_reputation','weak_foot','skill_moves','work_rate','body_type','real_face','release_clause_eur','player_tags','team_position','team_jersey_number','loaned_from','joined','contract_valid_until','nation_position','nation_jersey_number','pace','shooting','passing','dribbling','defending','physic','gk_diving','gk_handling','gk_kicking','gk_reflexes','gk_speed','gk_positioning','player_traits','attacking_crossing','attacking_finishing','attacking_heading_accuracy','attacking_short_passing','attacking_volleys','skill_dribbling','skill_curve','skill_fk_accuracy','skill_long_passing','skill_ball_control','movement_acceleration','movement_sprint_speed','movement_agility','movement_reactions','movement_balance','power_shot_power','power_jumping','power_stamina','power_strength','power_long_shots','mentality_aggression','mentality_interceptions','mentality_positioning','mentality_vision','mentality_penalties','mentality_composure','defending_marking','defending_standing_tackle','defending_sliding_tackle','goalkeeping_diving','goalkeeping_handling','goalkeeping_kicking','goalkeeping_positioning','goalkeeping_reflexes','ls','st','rs','lw','lf','cf','rf','rw','lam','cam','ram','lm','lcm','cm','rcm','rm','lwb','ldm','cdm','rdm','rwb','lb','lcb','cb','rcb','rb','year']
    dropped = list(variables[i] for i in [22,23,26,30,37,38,39,40,41,42,43])
    for v in dropped:
        full_df = full_df.drop(col(v))
    skills = ['attacking_crossing','attacking_finishing','attacking_heading_accuracy','attacking_short_passing','attacking_volleys','skill_dribbling','skill_curve','skill_fk_accuracy','skill_long_passing','skill_ball_control','movement_acceleration','movement_sprint_speed','movement_agility','movement_reactions','movement_balance','power_shot_power','power_jumping','power_stamina','power_strength','power_long_shots','mentality_aggression','mentality_interceptions','mentality_positioning','mentality_vision','mentality_penalties','mentality_composure','defending_marking','defending_standing_tackle','defending_sliding_tackle','goalkeeping_diving','goalkeeping_handling','goalkeeping_kicking','goalkeeping_positioning','goalkeeping_reflexes','ls','st','rs','lw','lf','cf','rf','rw','lam','cam','ram','lm','lcm','cm','rcm','rm','lwb','ldm','cdm','rdm','rwb','lb','lcb','cb','rcb','rb']
    for name in skills:
        main = name + '_main'
        plus = name + '_plus'
        minus = name + '_minus'
        new = name + '_c'
        full_df = full_df\
            .withColumn(main,regexp_extract(col(name), '^([0-9]+)', 1).cast("integer"))\
            .withColumn("tmp1",regexp_extract(col(name), '^([0-9]+)\+([0-9]+)', 2).cast("integer"))\
            .withColumn("tmp2",regexp_extract(col(name), '^([0-9]+)\-([0-9]+)', 2).cast("integer"))\
            .withColumn(plus, when(col('tmp1').isNull(),0).otherwise(col('tmp1')).cast("integer"))\
            .withColumn(minus, when(col('tmp2').isNull(),0).otherwise(col('tmp2')).cast("integer"))\
            .withColumn(new, col(main)+col(plus)-col(minus))
    # here cast integer is important since it will transform "" to Null
        full_df = full_df\
            .drop(col(main))\
            .drop(col('tmp1'))\
            .drop(col('tmp2'))\
            .drop(col(plus))\
            .drop(col(minus))\
            .drop(col(name))\
            .withColumnRenamed(new,name)
    imputer = Imputer (
    inputCols=skills,
    outputCols=["{}_imputed".format(c) for c in skills]
    ).setStrategy("mean").setMissingValue(0)
    full_imputed_df = imputer.fit(full_df).transform(full_df)
    for name in skills:
        newname = name + '_imputed'
        full_imputed_df = full_imputed_df.drop(name)
        full_imputed_df = full_imputed_df.withColumnRenamed(newname,name)
    db_properties={}
    db_properties['username']="postgres"
    db_properties['password']="990331"
    # make sure to use the correct port number. These 
    db_properties['url']= "jdbc:postgresql://localhost:5432/postgres"
    db_properties['driver']="org.postgresql.Driver"
    full_imputed_df.write.format("jdbc")\
    .mode("overwrite")\
    .option("url", "jdbc:postgresql://localhost:5432/postgres")\
    .option("dbtable", "fifa.player_info")\
    .option("user", "postgres")\
    .option("password", "990331")\
    .option("Driver", "org.postgresql.Driver")\
    .save()
    return full_imputed_df
task1()
