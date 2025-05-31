from pyspark.sql import SparkSession, functions as F
  
from pyspark.sql.types import DateType
  
from datetime import datetime
  

  
spark = SparkSession.builder \
  
    .appName("spMaintHistoryUpdate_Transform_Postgres") \
  
    .config("spark.jars", "/path/postgresql-<version>.jar") \  # Replace with JDBC driver path
  
    .getOrCreate()
  

  
loc_curr_date = datetime.utcnow().date()
  

  
# PostgreSQL connection properties
  
#postgres_url = "jdbc:postgresql://<hostname>:<port>/<dbname>"
postgres_url = "jdbc:postgresql:database-1-instance-1.cv664862kuy4.us-west-2.rds.amazonaws.com:5432/postgres"  # Replace with PostgreSQL connection info
  
properties = {
  
    "user": "postgres",  # PostgreSQL user
  
    "password": "Suryasan123",  # PostgreSQL password
  
    "driver": "org.postgresql.Driver"
  
}
  

  
# Table names (update with your actual table names)
  
tbl_rs_turc = "dbo.tblRadioStationTrackUser_RollingCounts"
  
tbl_rs_turc_counts = "dbo.tblRadioStationTrackUser_Counts"
  
tbl_rs_trc = "dbo.tblRadioStationTrack_RollingCounts"
  
tbl_trc = "dbo.tblTrack_RollingCounts"
  
tbl_tru = "dbo.tblTrackUser_RollingCounts"
  

  
# Load data from PostgreSQL tables
  
tblRadioStationTrackUser_RollingCounts = spark.read.jdbc(url=postgres_url, table=tbl_rs_turc, properties=properties)
  
tblRadioStationTrackUser_Counts = spark.read.jdbc(url=postgres_url, table=tbl_rs_turc_counts, properties=properties)
  
tblRadioStationTrack_RollingCounts = spark.read.jdbc(url=postgres_url, table=tbl_rs_trc, properties=properties)
  
tblTrack_RollingCounts = spark.read.jdbc(url=postgres_url, table=tbl_trc, properties=properties)
  
tblTrackUser_RollingCounts = spark.read.jdbc(url=postgres_url, table=tbl_tru, properties=properties)
  

  
# 1.1 rollover tblRadioStationTrackUser_RollingCounts
  
rs_turc_filtered = tblRadioStationTrackUser_RollingCounts.filter(F.col("current_date") != F.lit(loc_curr_date))
  

  
counts_filtered = tblRadioStationTrackUser_Counts.filter(
  
    (F.col("date_type") == "D") &
  
    (F.col("startdate") > F.date_sub(F.lit(loc_curr_date), 40))
  
)
  

  
agg_counts = (
  
    rs_turc_filtered.alias("c")
  
    .join(
  
        counts_filtered.alias("uc"),
  
        on=["group_id", "station_id", "track_id", "user_id"],
  
        how="inner"
  
    )
  
    .groupBy("c.group_id", "c.station_id", "c.track_id", "c.user_id")
  
    .agg(
  
        F.sum(F.when(F.col("uc.startdate") > F.date_sub(F.lit(loc_curr_date), 7), F.col("uc.streams_count")).otherwise(0)).alias("current_week_plays"),
  
        F.sum(F.when(F.col("uc.startdate") > F.date_sub(F.lit(loc_curr_date), 7), F.col("uc.skips_count")).otherwise(0)).alias("current_week_skips"),
  
        F.sum(F.when(F.col("uc.startdate") > F.date_sub(F.lit(loc_curr_date), 7), F.col("uc.likes_count")).otherwise(0)).alias("current_week_likes"),
  
        F.sum(F.when(F.col("uc.startdate") > F.date_sub(F.lit(loc_curr_date), 7), F.col("uc.dislikes_count")).otherwise(0)).alias("current_week_dislikes"),
  
        F.sum(F.when(F.col("uc.startdate") > F.date_sub(F.lit(loc_curr_date), 7), F.col("uc.shares_count")).otherwise(0)).alias("current_week_shares"),
  
        F.sum(F.when(F.col("uc.startdate") > F.date_sub(F.lit(loc_curr_date), 30), F.col("uc.streams_count")).otherwise(0)).alias("current_month_plays"),
  
        F.sum(F.when(F.col("uc.startdate") > F.date_sub(F.lit(loc_curr_date), 30), F.col("uc.skips_count")).otherwise(0)).alias("current_month_skips"),
  
        F.sum(F.when(F.col("uc.startdate") > F.date_sub(F.lit(loc_curr_date), 30), F.col("uc.likes_count")).otherwise(0)).alias("current_month_likes"),
  
        F.sum(F.when(F.col("uc.startdate") > F.date_sub(F.lit(loc_curr_date), 30), F.col("uc.dislikes_count")).otherwise(0)).alias("current_month_dislikes"),
  
        F.sum(F.when(F.col("uc.startdate") > F.date_sub(F.lit(loc_curr_date), 30), F.col("uc.shares_count")).otherwise(0)).alias("current_month_shares")
  
    )
  
)
  

  
updated_rs_turc = (
  
    rs_turc_filtered.alias("c")
  
    .join(
  
        agg_counts.alias("a"),
  
        on=["group_id", "station_id", "track_id", "user_id"],
  
        how="left"
  
    )
  
    .select(
  
        "c.group_id",
  
        "c.station_id",
  
        "c.track_id",
  
        "c.user_id",
  
        F.lit(loc_curr_date).cast(DateType()).alias("current_date"),
  
        F.lit(0).alias("current_day_plays"),
  
        F.lit(0).alias("current_day_skips"),
  
        F.lit(0).alias("current_day_likes"),
  
        F.lit(0).alias("current_day_dislikes"),
  
        F.lit(0).alias("current_day_shares"),
  
        F.coalesce(F.col("a.current_week_plays"), F.lit(0)).alias("current_week_plays"),
  
        F.coalesce(F.col("a.current_week_skips"), F.lit(0)).alias("current_week_skips"),
  
        F.coalesce(F.col("a.current_week_likes"), F.lit(0)).alias("current_week_likes"),
  
        F.coalesce(F.col("a.current_week_dislikes"), F.lit(0)).alias("current_week_dislikes"),
  
        F.coalesce(F.col("a.current_week_shares"), F.lit(0)).alias("current_week_shares"),
  
        F.coalesce(F.col("a.current_month_plays"), F.lit(0)).alias("current_month_plays"),
  
        F.coalesce(F.col("a.current_month_skips"), F.lit(0)).alias("current_month_skips"),
  
        F.coalesce(F.col("a.current_month_likes"), F.lit(0)).alias("current_month_likes"),
  
        F.coalesce(F.col("a.current_month_dislikes"), F.lit(0)).alias("current_month_dislikes"),
  
        F.coalesce(F.col("a.current_month_shares"), F.lit(0)).alias("current_month_shares"),
  
    )
  
)
  

  
# 1.2 rollover tblRadioStationTrack_RollingCounts
  
rs_trc_filtered = tblRadioStationTrack_RollingCounts.filter(F.col("current_date") != F.lit(loc_curr_date))
  

  
agg_user_rolling = (
  
    rs_trc_filtered.alias("c")
  
    .join(
  
        updated_rs_turc.alias("uc"),
  
        on=["group_id", "station_id", "track_id"],
  
        how="inner"
  
    )
  
    .groupBy("c.group_id", "c.station_id", "c.track_id")
  
    .agg(
  
        F.sum("uc.current_week_plays").alias("current_week_plays"),
  
        F.sum("uc.current_week_skips").alias("current_week_skips"),
  
        F.sum("uc.current_week_likes").alias("current_week_likes"),
  
        F.sum("uc.current_week_dislikes").alias("current_week_dislikes"),
  
        F.sum("uc.current_week_shares").alias("current_week_shares"),
  
        F.sum(F.when(F.col("uc.current_week_plays") > 0, 1).otherwise(0)).alias("current_week_users"),
  
        F.sum(F.when(F.col("uc.current_week_skips") > 0, 1).otherwise(0)).alias("current_week_skipusers"),
  
        F.sum(F.when(F.col("uc.current_week_likes") > 0, 1).otherwise(0)).alias("current_week_likeusers"),
  
        F.sum(F.when(F.col("uc.current_week_dislikes") > 0, 1).otherwise(0)).alias("current_week_dislikeusers"),
  
        F.sum(F.when(F.col("uc.current_week_shares") > 0, 1).otherwise(0)).alias("current_week_shareusers"),
  
        F.sum("uc.current_month_plays").alias("current_month_plays"),
  
        F.sum("uc.current_month_skips").alias("current_month_skips"),
  
        F.sum("uc.current_month_likes").alias("current_month_likes"),
  
        F.sum("uc.current_month_dislikes").alias("current_month_dislikes"),
  
        F.sum("uc.current_month_shares").alias("current_month_shares"),
  
        F.sum(F.when(F.col("uc.current_month_plays") > 0, 1).otherwise(0)).alias("current_month_users"),
  
        F.sum(F.when(F.col("uc.current_month_skips") > 0, 1).otherwise(0)).alias("current_month_skipusers"),
  
        F.sum(F.when(F.col("uc.current_month_likes") > 0, 1).otherwise(0)).alias("current_month_likeusers"),
  
        F.sum(F.when(F.col("uc.current_month_dislikes") > 0, 1).otherwise(0)).alias("current_month_dislikeusers"),
  
        F.sum(F.when(F.col("uc.current_month_shares") > 0, 1).otherwise(0)).alias("current_month_shareusers"),
  
    )
  
)
  

  
updated_rs_trc = (
  
    rs_trc_filtered.alias("c")
  
    .join(
  
        agg_user_rolling.alias("a"),
  
        on=["group_id", "station_id", "track_id"],
  
        how="left"
  
    )
  
    .select(
  
        "c.group_id",
  
        "c.station_id",
  
        "c.track_id",
  
        F.lit(loc_curr_date).cast(DateType()).alias("current_date"),
  
        F.lit(0).alias("current_day_plays"),
  
        F.lit(0).alias("current_day_skips"),
  
        F.lit(0).alias("current_day_likes"),
  
        F.lit(0).alias("current_day_dislikes"),
  
        F.lit(0).alias("current_day_shares"),
  
        F.lit(0).alias("current_day_users"),
  
        F.lit(0).alias("current_day_skipusers"),
  
        F.lit(0).alias("current_day_likeusers"),
  
        F.lit(0).alias("current_day_dislikeusers"),
  
        F.lit(0).alias("current_day_shareusers"),
  
        F.coalesce(F.col("a.current_week_plays"), F.lit(0)).alias("current_week_plays"),
  
        F.coalesce(F.col("a.current_week_skips"), F.lit(0)).alias("current_week_skips"),
  
        F.coalesce(F.col("a.current_week_likes"), F.lit(0)).alias("current_week_likes"),
  
        F.coalesce(F.col("a.current_week_dislikes"), F.lit(0)).alias("current_week_dislikes"),
  
        F.coalesce(F.col("a.current_week_shares"), F.lit(0)).alias("current_week_shares"),
  
        F.coalesce(F.col("a.current_week_users"), F.lit(0)).alias("current_week_users"),
  
        F.coalesce(F.col("a.current_week_skipusers"), F.lit(0)).alias("current_week_skipusers"),
  
        F.coalesce(F.col("a.current_week_likeusers"), F.lit(0)).alias("current_week_likeusers"),
  
        F.coalesce(F.col("a.current_week_dislikeusers"), F.lit(0)).alias("current_week_dislikeusers"),
  
        F.coalesce(F.col("a.current_week_shareusers"), F.lit(0)).alias("current_week_shareusers"),
  
        F.coalesce(F.col("a.current_month_plays"), F.lit(0)).alias("current_month_plays"),
  
        F.coalesce(F.col("a.current_month_skips"), F.lit(0)).alias("current_month_skips"),
  
        F.coalesce(F.col("a.current_month_likes"), F.lit(0)).alias("current_month_likes"),
  
        F.coalesce(F.col("a.current_month_dislikes"), F.lit(0)).alias("current_month_dislikes"),
  
        F.coalesce(F.col("a.current_month_shares"), F.lit(0)).alias("current_month_shares"),
  
        F.coalesce(F.col("a.current_month_users"), F.lit(0)).alias("current_month_users"),
  
        F.coalesce(F.col("a.current_month_skipusers"), F.lit(0)).alias("current_month_skipusers"),
  
        F.coalesce(F.col("a.current_month_likeusers"), F.lit(0)).alias("current_month_likeusers"),
  
        F.coalesce(F.col("a.current_month_dislikeusers"), F.lit(0)).alias("current_month_dislikeusers"),
  
        F.coalesce(F.col("a.current_month_shareusers"), F.lit(0)).alias("current_month_shareusers"),
  
    )
  
)
  

  
# 2.1 update wilson_score lower bound confidence intervals on tblRadioStationTrack_RollingCounts
  

  
def fnCalculateWilsonScore(pos, neg):
  
    # Placeholder: replicate your SQL scalar UDF call
  
    return 0.0
  

  
from pyspark.sql.types import DoubleType
  
from pyspark.sql.functions import udf
  

  
wilson_score_udf = udf(fnCalculateWilsonScore, DoubleType())
  

  
joined_wilson_trc = (
  
    updated_rs_trc.alias("rtrc")
  
    .join(
  
        tblTrack_RollingCounts.alias("trc"),
  
        on=["group_id", "track_id"],
  
        how="inner"
  
    )
  
    .select(
  
        "rtrc.group_id",
  
        "rtrc.station_id",
  
        "rtrc.track_id",
  
        wilson_score_udf(
  
            (F.col("trc.current_month_users") * 0.1) +
  
            (F.col("rtrc.current_day_users") * 0.8) +
  
            F.col("rtrc.current_week_likeusers") +
  
            (F.col("rtrc.current_month_users") * 0.5) +
  
            F.col("rtrc.total_likes") +
  
            F.lit(0.001),
  
            (F.col("trc.current_month_skipusers") * 0.2) +
  
            F.col("rtrc.current_day_skipusers") +
  
            F.col("rtrc.current_week_dislikeusers") +
  
            F.col("rtrc.current_month_skipusers") +
  
            F.col("rtrc.total_dislikes") +
  
            F.lit(0.001)
  
        ).alias("wilson_score")
  
    )
  
)
  

  
final_rs_trc = (
  
    updated_rs_trc.alias("c")
  
    .join(
  
        joined_wilson_trc.alias("t"),
  
        on=["group_id", "station_id", "track_id"],
  
        how="left"
  
    )
  
    .select(
  
        "c.*",
  
        F.coalesce(F.col("t.wilson_score"), F.lit(0)).alias("wilson_score")
  
    )
  
)
  

  
# 2.2 update wilson_score lower bound confidence intervals on tblRadioStationTrackUser_RollingCounts
  

  
joined_wilson_turc = (
  
    updated_rs_turc.alias("rtrc")
  
    .join(
  
        tblTrackUser_RollingCounts.alias("trc"),
  
        on=["group_id", "track_id", "user_id"],
  
        how="inner"
  
    )
  
    .select(
  
        "rtrc.group_id",
  
        "rtrc.station_id",
  
        "rtrc.track_id",
  
        "rtrc.user_id",
  
        wilson_score_udf(
  
            (F.col("trc.current_month_plays") * 0.1) +
  
            (F.col("rtrc.current_day_plays") * 0.8) +
  
            F.col("rtrc.current_week_likes") +
  
            (F.col("rtrc.current_month_plays") * 0.5) +
  
            F.col("rtrc.total_likes") +
  
            F.lit(0.001),
  
            (F.col("trc.current_month_skips") * 0.2) +
  
            F.col("rtrc.current_day_skips") +
  
            F.col("rtrc.current_week_dislikes") +
  
            F.col("rtrc.current_month_skips") +
  
            F.col("rtrc.total_dislikes") +
  
            F.lit(0.001)
  
        ).alias("wilson_score")
  
    )
  
)
  

  
final_rs_turc = (
  
    updated_rs_turc.alias("c")
  
    .join(
  
        joined_wilson_turc.alias("t"),
  
        on=["group_id", "station_id", "track_id", "user_id"],
  
        how="left"
  
    )
  
    .select(
  
        "c.*",
  
        F.coalesce(F.col("t.wilson_score"), F.lit(0)).alias("wilson_score")
  
    )
  
)
  

  
# Save final DataFrames back to Parquet
  
#final_rs_turc.write.mode("overwrite").parquet(path_rs_turc)
  
final_rs_turc.write.jdbc(url=postgres_url, table=tbl_rs_turc, mode="overwrite", properties=properties)
  
#final_rs_trc.write.mode("overwrite").parquet(path_rs_trc)
  
final_rs_trc.write.jdbc(url=postgres_url, table=tbl_rs_trc, mode="overwrite", properties=properties)
  

  
print("spMaintHistoryUpdate procedure translated and executed successfully.")
  

  
spark.stop()
