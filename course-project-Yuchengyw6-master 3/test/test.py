from pyspark.sql.functions import regexp_extract, col
import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import to_date, col, count, when, isnan, regexp_replace
from pyspark.ml.feature import Imputer
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf, SQLContext
import os

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
# Parameters: year1, year2, x
# year1, year2, the year used to select the players who improve the most. year2 is the more recent year
# x, number of players that will be selected
# Return: return list of players who improve the most
# Description: The function select top x players who improve the most.
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
def task2_4_1(year = "all"):
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
        df = full_imputed_df_read.groupBy("nation_position").count().sort(col("count").desc())
        collect = df.collect()
    except ValueError:
        print("Something wrong in Calculation")
    if collect[0][0]==None:
        return collect[1][0]
    else:
        return collect[0][0]
def task2_4_2(year="all"):
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
        df = full_imputed_df_read.groupBy("team_position").count().sort(col("count").desc())
        collect = df.collect()
    except ValueError:
        print("Something wrong in Calculation")
    if collect[0][0]==None:
        return collect[1][0]
    else:
        return collect[0][0]
def task2_5(year = "all"):
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
        df = full_imputed_df_read.groupBy("nationality").count().sort(col("count").desc())
        collect = df.collect()
    except ValueError:
        print("Something wrong in calculation")
    return collect[0][0]
import pytest
# DESCRIPTION: test if task1 has correct number of rows and if task1 return a database
def test_task1():
    data = task1()
    assert data is not None, "Failed to load data"
    assert data.count() == 100995,"Wrong row number"
# DESCRIPTION: test if task2_1 return correct subset when the input is valid
def test_task2_1_happy():
    assert task2_1(2015,2016,5) == ['Scott Brown', 'Adam Smith', 'Mustafa Akbaş', 'Alvin Arrondel', 'Liam Kelly'], "Wrong Output"
    assert task2_1(2015,2019,5) == ['Liam Kelly',
 'Adam Smith',
 'Jorge Rodríguez',
 'Tom Davies',
 'Abdulrahman Al Ghamdi'], "Wrong Output"
    assert task2_1(2016,2017,5) == ['Scott Brown', 'Danny Ward', 'Alan Smith', 'Jorge Rodríguez', 'Adam Smith'],"Wrong Output"
    assert task2_1(2015,2020,5) ==['Adam Smith',
 'Liam Kelly',
 'Tom Davies',
 'Mohammed Al Buraik',
 'Abdulrahman Al Ghamdi'] , "Wrong Output"
# DESCRIPTION: test if task2_1 can return correct notification when the input is not valid
def test_task2_1_sad():
    assert task2_1("Who are you",2016,5) == "Invalid", "Wrong Output"
    assert task2_1(2020,2015,5) == "Invalid", "Wrong Output"
    assert task2_1(2020,2015,0.2) == "Invalid", "Wrong Output"
    assert task2_1(2020,2015,"apple") == "Invalid", "Wrong Output"
# DESCRIPTION: test if task2_2 can return correct subset when the input is valid
def test_task2_2_happy():
    assert task2_2(5) == ['Boyacá Chicó FC',
 'FC Girondins de Bordeaux',
 'Club Atlético Banfield',
 'River Plate',
 'Querétaro'],"Wrong Output"
    assert task2_2(7) == ['Boyacá Chicó FC',
 'FC Girondins de Bordeaux',
 'Club Atlético Banfield',
 'River Plate',
 'Querétaro',
 'Club América',
 'Newcastle United'],"Wrong Output"
# DESCRIPTION: test if task2_2 can return correct notification when the input is not valid
def test_task2_2_sad():
    assert task2_2(0) == "Invalid" ,"Wrong Output"
    assert task2_2("ss") == "Invalid" ,"Wrong Output"
# DESCRIPTION: test if task2_3 can return correct subset when the input is valid
def test_task2_3_happy():
    assert task2_3(10,"all") == ['Arsenal',
 'Southampton',
 'Leicester City',
 'Tottenham Hotspur',
 'West Ham United',
 'Crystal Palace',
 'Manchester City',
 'Lazio',
 'Chelsea', 
 'Everton'],"Wrong Output"
    assert task2_3(10,2020) == 'All the same',"Wrong Output"
    assert task2_3(10,2019) == 'All the same',"Wrong Output"
    assert task2_3(6) == ['Arsenal',
 'Southampton',
 'Leicester City',
 'Tottenham Hotspur',
 'West Ham United',
 'Crystal Palace']
# DESCRIPTION: test if task2_3 can return correct notification when the input is not valid
def test_task2_3_sad():
    assert task2_3(10,"alffasdl") == "invalid input","Wrong Output"
    assert task2_3(1,2020) == 'z is not big enough',"Wrong Output"
    assert task2_3("d",2019) == 'Invalid',"Wrong Output"
# DESCRIPTION: test if task2_4_1 can return correct value when the input is valid
def test_task2_4_1_happy():
    assert task2_4_1(2019) == 'SUB',"Wrong Output"
    assert task2_4_1() == 'SUB',"Wrong Output"
    assert task2_4_1(2017) == 'SUB',"Wrong Output"
# DESCRIPTION: test if task2_4_1 can return correct notification when the input is not valid
def test_task2_4_1_sad():
    assert task2_4_1("df") == "invalid input","Wrong Output"
    assert task2_4_1(21) == "invalid input","Wrong Output"
    assert task2_4_1(2038) == "invalid input","Wrong Output"
# DESCRIPTION: test if task2_4_2 can return correct value when the input is valid
def test_task2_4_2_happy():
    assert task2_4_2(2019) == 'SUB',"Wrong Output"
    assert task2_4_2() == 'SUB',"Wrong Output"
    assert task2_4_2(2017) == 'SUB',"Wrong Output"
# DESCRIPTION: test if task2_4_2 can return correct notification when the input is not valid
def test_task2_4_2_sad():
    assert task2_4_2("Df") == "invalid input","Wrong Output"
    assert task2_4_2(22) == "invalid input","Wrong Output"
    assert task2_4_2(3333) == "invalid input","Wrong Output"
# DESCRIPTION: test if task2_5 can return correct value when the input is valid
def test_task2_5_happy():
    assert task2_5(2019) == 'England',"Wrong Output"
    assert task2_5() == 'England',"Wrong Output"
    assert task2_5(2017) == 'England',"Wrong Output"
# DESCRIPTION: test if task2_5 can return correct notification when the input is not valid
def test_task2_5_sad():
    assert task2_5("df") == "invalid input","Wrong Output"
    assert task2_5(21) == "invalid input","Wrong Output"
    assert task2_5(2038) == "invalid input","Wrong Output"
