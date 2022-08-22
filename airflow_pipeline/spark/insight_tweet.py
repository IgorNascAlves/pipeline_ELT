import argparse
from os.path import join

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

def get_user_data(df_tweet, user_id):
    return df_tweet\
        .where(f"author_id = '{user_id}'")\
        .select("author_id", "conversation_id")


def get_tweet_conversas(df_tweet, df_user, user_id):
    return df_tweet.alias("tweet")\
        .join(
            df_user.alias("nba_brasil"),
            [
                df_user.author_id != df_tweet.author_id,
                df_user.conversation_id == df_tweet.conversation_id
            ],
            'left')\
        .withColumn(
            "nba_brasil_conversation",
            f.when(f.col("nba_brasil.conversation_id").isNotNull(), 1).otherwise(0)
        ).withColumn(
            "reply_nba_brasil",
            f.when(f.col("tweet.in_reply_to_user_id") == user_id, 1).otherwise(0)
        ).groupBy(f.to_date("created_at").alias("created_date"))\
        .agg(
            f.countDistinct("id").alias("n_tweets"),
            f.countDistinct("tweet.conversation_id").alias("n_conversation"),
            f.sum("nba_brasil_conversation").alias("nba_brasil_conversation"),
            f.sum("reply_nba_brasil").alias("reply_alura")
        ).withColumn("weekday", f.date_format("created_date", "E"))

def export_json(df, dest):
    df.coalesce(1).write.mode("overwrite").json(dest)

def twitter_insight(spark, src, dest, process_date, user_id):
    
    df_tweet = spark.read.json(join(src, 'tweet'))
    df_user = get_user_data(df_tweet, user_id)

    tweet_conversas = get_tweet_conversas(df_tweet, df_user, user_id)

    export_json(tweet_conversas, join(dest, f"process_date={process_date}"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spark Twitter Transformation"
    )
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--process-date", required=True)
    parser.add_argument("--user-id", required=True)
    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()

    twitter_insight(spark, args.src, args.dest, args.process_date, args.user_id)