from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, hour, row_number, desc
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Music Listening Behavior Analysis") \
    .getOrCreate()

# Load datasets
logs = spark.read.option("header", True).csv("input/listening_logs.csv", inferSchema=True)
meta = spark.read.option("header", True).csv("input/songs_metadata.csv", inferSchema=True)

# Convert timestamp to timestamp type
logs = logs.withColumn("timestamp", col("timestamp").cast("timestamp"))
logs = logs.withColumn("duration_sec", col("duration_sec").cast("int"))

# Join datasets
logs_meta = logs.join(meta, on="song_id")

# 1. Each user's favorite genre
from pyspark.sql.functions import row_number

window_user_genre = Window.partitionBy("user_id").orderBy(desc("count"))
user_favorite_genres = logs_meta.groupBy("user_id", "genre").count() \
    .withColumn("rank", row_number().over(window_user_genre)) \
    .filter("rank = 1") \
    .drop("rank")

user_favorite_genres.write.option("header", True).csv("output/user_favorite_genres")

# 2. Average listen time per song
avg_listen_time = logs.groupBy("song_id").agg(avg("duration_sec").alias("avg_duration"))
avg_listen_time.write.option("header", True).csv("output/avg_listen_time_per_song")

# 3. Top 10 most played songs this week
one_week_ago = datetime.now() - timedelta(days=7)
top_songs_this_week = logs.filter(col("timestamp") >= one_week_ago) \
    .groupBy("song_id").count() \
    .orderBy(desc("count")).limit(10)

top_songs_this_week.write.option("header", True).csv("output/top_songs_this_week")

# 4. Recommend “Happy” songs to Sad listeners
# Step 1: Identify sad users
sad_plays = logs_meta.filter(col("mood") == "Sad").groupBy("user_id").count().filter("count > 5")

# Step 2: Get happy songs
happy_songs = meta.filter(col("mood") == "Happy").select("song_id", "title", "artist")

# Step 3: Find which songs sad users already played
played = logs.select("user_id", "song_id").distinct()

# Step 4: Recommend songs not played yet
recommendations = sad_plays.select("user_id").crossJoin(happy_songs) \
    .join(played, on=["user_id", "song_id"], how="left_anti") \
    .withColumn("rank", row_number().over(Window.partitionBy("user_id").orderBy("song_id"))) \
    .filter("rank <= 3")

recommendations.write.option("header", True).csv("output/happy_recommendations")

# 5. Genre loyalty score
#######
genre_counts = logs_meta.groupBy("user_id", "genre").count()

# 2. Count total number of songs each user listened to
total_counts = logs_meta.groupBy("user_id").count().withColumnRenamed("count", "total")

# 3. Join counts and calculate loyalty score
joined = genre_counts.join(total_counts, on="user_id")

# 4. Compute loyalty score and rank
loyalty = joined.withColumn("loyalty_score", col("count") / col("total")) \
    .withColumn("rank", row_number().over(Window.partitionBy("user_id").orderBy(desc("loyalty_score")))) \
    .filter((col("rank") == 1) & (col("loyalty_score") > 0.8)) \
    .select("user_id", "genre", "loyalty_score")

# 5. Save the output
loyalty.write.mode("overwrite").option("header", True).csv("output/genre_loyalty_scores")
#########
# 6. Night owl users (listening between 12 AM - 5 AM)
night_owls = logs.withColumn("hour", hour("timestamp")) \
    .filter("hour >= 0 and hour < 5") \
    .select("user_id").distinct()

night_owls.write.option("header", True).csv("output/night_owl_users")

# Done
print("All tasks completed! Check the 'output' folder.")