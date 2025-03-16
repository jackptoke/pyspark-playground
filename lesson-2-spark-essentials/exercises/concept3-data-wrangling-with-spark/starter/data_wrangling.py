# Take care of any imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.functions import desc
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.functions import asc
from pyspark.sql import Window

from datetime import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Create the Spark Context
spark = SparkSession \
    .builder \
    .appName("Wrangling Data") \
    .getOrCreate()

# Complete the script
path = "../../data/sparkify_log_small.json"

user_log_df = spark.read.json(path)


# # Data Exploration 
some = user_log_df.take(5)
# # Explore the data set.


# View 5 records 
print(some)
# Print the schema
user_log_df.printSchema()

# Describe the dataframe
user_log_df.describe().show()

# Describe the statistics for the song length column
user_log_df.describe("song").show()

# Count the rows in the dataframe
print(f"Number of rows: {user_log_df.count()}")

# Select the page column, drop the duplicates, and sort by page
user_log_df.select("page").dropDuplicates().sort("page").show()

print(f"Number of rows after some rows dropped: {user_log_df.count()}")
# Select data for all pages where userId is 1046
result = user_log_df.select("*").where(user_log_df.userId == "1046").collect()

print(f"Number of records with userId 1046: {len(result)}")
# # Calculate Statistics by Hour

get_hour = udf(lambda x: datetime.fromtimestamp(x / 1000.0). hour)
user_log_df = user_log_df.withColumn("hour", get_hour(user_log_df.ts))
user_log_df.describe().show()
# Select just the NextSong page

nex_song_page = user_log_df.filter(user_log_df.page == "NextSong").groupby(user_log_df.hour).count().orderBy(user_log_df.hour.cast("float"))

nex_song_page.describe().show()
# # Drop Rows with Missing Values
user_log_valid_df = user_log_df.dropna(how = "any", subset = ["userId", "sessionId"])

# How many are there now that we dropped rows with null userId or sessionId?
print("Number of none null rows: ", user_log_valid_df.count())

# select all unique user ids into a dataframe
# Select only data for where the userId column isn't an empty string (different from null)
user_log_df.select("userId").filter(user_log_df.userId != "").dropDuplicates().sort("userId").show()

# # Users Downgrade Their Accounts
# 
# Find when users downgrade their accounts and then show those log entries.
print("People who downgraded: ")
user_log_df.filter(user_log_df.page == "Submit Downgrade").show()


# Create a user defined function to return a 1 if the record contains a downgrade
is_downgrade = udf(lambda page: 1 if page == "Submit Downgrade" else 0, IntegerType())

# Select data including the user defined function
print("User defined filter: ")
user_log_downgraded = user_log_df.select("*").withColumn("downgraded", is_downgrade("page"))
user_log_downgraded.head()

# Partition by user id
# Then use a window function and cumulative sum to distinguish each user's data as either pre or post downgrade events.
windowval = Window.partitionBy("userId").orderBy(desc("ts")).rangeBetween(Window.unboundedPreceding, 0)
# Fsum is a cumulative sum over a window - in this case a window showing all events for a user
# Add a column called phase, 0 if the user hasn't downgraded yet, 1 if they have
user_log_valid = user_log_downgraded.withColumn("phase", Fsum("downgraded").over(windowval))

# Show the phases for user 1138 
user_log_valid.select(["userId", "firstname", "ts", "page", "level", "phase"]).where(user_log_df.userId == "1138").sort("ts").show()
