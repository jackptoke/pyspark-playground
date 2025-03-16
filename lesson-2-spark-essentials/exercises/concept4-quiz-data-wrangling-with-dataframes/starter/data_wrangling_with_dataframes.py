# # Data Wrangling with DataFrames Coding Quiz

from pyspark.sql import SparkSession
from pyspark.sql.functions import asc, desc, udf, count, col
from pyspark.sql import Window
from pyspark.sql.types import IntegerType

# TODOS: 
# 1) import any other libraries you might need
# 2) instantiate a Spark session 
# 3) read in the data set located at the path "../../data/sparkify_log_small.json"
# 4) write code to answer the quiz questions 

spark = SparkSession \
    .builder \
    .appName("Data Frames practice") \
    .getOrCreate()

path = "../../data/sparkify_log_small.json"

users_df = spark.read.json(path)

# # Question 1
# 
# Which page did user id "" (empty string) NOT visit?

# My approad
print("My solution:")
all_pages = users_df.select('page').drop_duplicates()
pages_visited = users_df.filter(users_df.userId == '').select('page').drop_duplicates()
all_pages.subtract(pages_visited).orderBy(asc("page")).show()



# # Question 2 - Reflect
# 
# What type of user does the empty string user id most likely refer to?
# 
# Users who have not signed up


# # Question 3
# 
# How many female users do we have in the data set?
number_of_female = users_df.filter(users_df.gender == 'F').select("userId").drop_duplicates().count()
print(number_of_female)


# # Question 4
# 
# How many songs were played from the most played artist?
users_df.filter(users_df.page == 'NextSong') \
    .select('Artist') \
    .groupBy('Artist') \
    .agg({'Artist':'count'}) \
    .withColumnRenamed('count(Artist)', 'Playcount') \
    .sort(desc('Playcount')) \
    .show(1)

# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
# 
#
user_window = Window \
    .partitionBy('userID') \
    .orderBy(desc('ts')) \
    .rangeBetween(Window.unboundedPreceding, 0)

is_home = udf(lambda page : int(page == 'Home'), IntegerType())

cum_songs = users_df.filter((users_df.page == 'NextSong') | (users_df.page == 'Home')) \
    .select("userId", "page", "ts") \
    .withColumn("Home Visits", is_home(col("page"))) \
    .withColumn("period", count("Home Visits").over(user_window))

cum_songs.filter(cum_songs.page == "NextSong") \
                .groupBy("userId", "period") \
                .agg({"period":"count"}) \
                .agg({"count(period)":"avg"}) \
                .show()
