#!/usr/bin/env python
# coding: utf-8

# # Data Wrangling with Spark SQL Quiz
# 
# This code uses the same dataset and most of the same questions from the earlier code using dataframes. For this scropt, however, use Spark SQL instead of Spark Data Frames.
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, asc
from matplotlib import pyplot as plt
from datetime import datetime

from pyspark.sql.types import StringType

import pandas as pd

# TODOS: 
# 1) import any other libraries you might need
# 2) instantiate a Spark session
spark = SparkSession \
    .builder \
    .appName("Data wrangling with Spark SQL") \
    .getOrCreate()

# 3) read in the data set located at the path "data/sparkify_log_small.json"
data_path = "../../data/sparkify_log_small.json"

# 4) create a view to use with your SQL queries
user_logs = spark.read.json(data_path)

user_logs.createOrReplaceTempView("user_logs")

# 5) write code to answer the quiz questions 


# # Question 1
# 
# Which page did user id ""(empty string) NOT visit?
page_not_visited_by_anonymous_users = spark.sql("SELECT DISTINCT page FROM user_logs WHERE page NOT IN (SELECT DISTINCT page FROM user_logs WHERE userId = '') ORDER BY page ASC")
page_not_visited_by_anonymous_users.show()

# TODO: write your code to answer question 1


# # Question 2 - Reflect
# 
# Why might you prefer to use SQL over data frames? Why might you prefer data frames over SQL?
# More flexibility and familiarity with sQL

# # Question 3
# 
# How many female users do we have in the data set?
gender_counts = spark.sql("SELECT gender, COUNT(*) FROM (SELECT DISTINCT userId, gender FROM user_logs) GROUP BY gender")
gender_counts.show()


# # Question 4
# 
# How many songs were played from the most played artist?
spark.sql("SELECT Artist, COUNT(*) AS Playcount FROM user_logs WHERE page = 'NextSong' GROUP BY Artist ORDER BY Playcount DESC LIMIT 1").show()

# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
spark.sql("""
 SELECT ROUND(AVG(NumOfSongs), 0) 
 FROM (SELECT sessionId, COUNT(*) NumOfSongs FROM user_logs GROUP BY sessionId)
""").show()


to_hour = udf(lambda x: datetime.fromtimestamp(x/1000).strftime("%H"), StringType())
to_date = udf(lambda x: datetime.fromtimestamp(x/1000).strftime("%d-%m-%Y"), StringType())

song_played = user_logs.select("song", "ts") \
    .filter(user_logs.song != "") \
    .withColumn("date", to_date("ts")) \
    .withColumn("hour", to_hour("ts"))

song_played.show(10)

num_plays_per_hour = song_played.groupby(["date", "hour"]) \
    .agg({"*": "count"}) \
    .withColumnRenamed("count(1)", "NumOfTimesPlayed") \

num_plays_per_hour.show(24)

result = num_plays_per_hour.groupby("hour") \
    .agg({"NumOfTimesPlayed": "avg"}) \
    .withColumnRenamed("avg(NumOfTimesPlayed)", "NumOfSongs by Hour") \
    .sort(asc("hour"))

result.show()

result_pd = result.toPandas()

plt.scatter(result_pd["hour"], result_pd["NumOfSongs by Hour"])
plt.xlim(-1, 24)
plt.ylim(0, 1.2 * max(result_pd["NumOfSongs by Hour"]))
plt.xlabel("Hour of the Day")
plt.ylabel("Number of Songs Played")
plt.savefig("num_songs_played_by_hour.png")
plt.show()