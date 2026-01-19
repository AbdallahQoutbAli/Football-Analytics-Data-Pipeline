# Databricks notebook source
# MAGIC %md
# MAGIC # ⚽ Manchester United Analytics Platform
# MAGIC ## Complete Data Pipeline: Transform & Load to Snowflake
# MAGIC
# MAGIC **Pipeline:** DBFS → PySpark Transformation → Snowflake DW  
# MAGIC **Data:** 900+ matches, 50M+ events  
# MAGIC **Stack:** Databricks + Snowflake

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# ⚠️ IMPORTANT: Set correct data path
DATA_PATH = "dbfs:/statsbomb-data"  # Your actual path!

print("⚽ Manchester United Analytics Platform")
print("="*60)
print(f"🔥 Spark Version: {spark.version}")
print(f"📂 Data Path: {DATA_PATH}")
print("="*60)

# COMMAND ----------

# MAGIC  %md
# MAGIC ## 🔐 Snowflake Configuration

# COMMAND ----------

SNOWFLAKE_OPTIONS = {
    "sfUrl": "ZQCYYEO-VG84200.snowflakecomputing.com",   
    "sfUser": "ABDALLAHMANNTD",           
    "sfPassword": "\"2]VaS4Cav]X8CR",                
    "sfDatabase": "FOOTBALL_ANALYTICS",
    "sfSchema": "MANUTD",
    "sfWarehouse": "ANALYTICS_WH"
}
 

# COMMAND ----------

# Test connection
print("🔍 Testing Snowflake connection...")

try:
    # Create simple test DataFrame
    test_df = spark.createDataFrame([(1, "test")], ["id", "name"])
    
    # Try to write
    test_df.write \
        .format("snowflake") \
        .options(**SNOWFLAKE_OPTIONS) \
        .option("dbtable", "CONNECTION_TEST") \
        .mode("overwrite") \
        .save()
    
    print("✅ Connection successful!")
    
    # Clean up test table
    # (You can manually delete it in Snowflake later)
    
except Exception as e:
    print(f"❌ Connection failed!")
    print(f"Error: {str(e)}")
    print("\n💡 Troubleshooting:")
    print("1. Check your account URL format")
    print("2. Verify username/password")
    print("3. Make sure database, schema, and warehouse exist in Snowflake")
    print("4. Check if warehouse is running (can be suspended)")
    print("\n📝 Run this in Snowflake to get your account info:")
    print("   SELECT CURRENT_ACCOUNT(), CURRENT_REGION();")
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 1. Load Competitions
# MAGIC

# COMMAND ----------

# Verify data exists
print("🔍 Verifying data location...")
try:
    files = dbutils.fs.ls(DATA_PATH)
    print(f"✅ Found {len(files)} items:")
    for f in files:
        print(f"   📁 {f.name}")
except Exception as e:
    print(f"❌ Error accessing {DATA_PATH}")
    print(f"   {e}")
    print("\n💡 Try checking the path in Data → Browse DBFS")

# COMMAND ----------

# Read competitions 
competitions_path = f"{DATA_PATH}/competitions.json"
print(f"📥 Reading: {competitions_path}")

competitions_df = (
    spark.read
    .option("multiLine", "true")
    .option("mode", "PERMISSIVE")
    .json(competitions_path)
)

print(f"✅ Competitions loaded: {competitions_df.count()}")
display(competitions_df.limit(5))

# COMMAND ----------

# Clean and save to Snowflake
competitions_clean = competitions_df.select(
    col("competition_id"),
    col("competition_name"),
    col("season_id"),
    col("season_name"),
    col("country_name").alias("country"),
    col("competition_gender").alias("gender")
)

# COMMAND ----------

# Write to Snowflake
print("📤 Writing to Snowflake...")
competitions_clean.write \
    .format("snowflake") \
    .options(**SNOWFLAKE_OPTIONS) \
    .option("dbtable", "DIM_COMPETITIONS") \
    .mode("overwrite") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚽ 2. Load All Matches

# COMMAND ----------

matches_path = f"{DATA_PATH}/matches/*/*.json"
print(f"📥 Reading: {matches_path}")

matches_df = (
    spark.read
    .option("multiLine", "true")
    .option("mode", "PERMISSIVE")
    .json(matches_path)
    .cache()
)

print(f"⚽ Total Matches: {matches_df.count():,}")
display(matches_df.limit(5))


# COMMAND ----------

# Transform matches
matches_clean = matches_df.select(
    col("match_id"),
    col("match_date"),
    col("kick_off"),
    col("match_week"),
    col("match_status"),
    
    # Competition
    col("competition.competition_id").alias("competition_id"),
    col("competition.competition_name").alias("competition_name"),
    
    # Season
    col("season.season_id").alias("season_id"),
    col("season.season_name").alias("season_name"),
    
    # Teams
    col("home_team.home_team_id").alias("home_team_id"),
    col("home_team.home_team_name").alias("home_team_name"),
    col("away_team.away_team_id").alias("away_team_id"),
    col("away_team.away_team_name").alias("away_team_name"),
    
    # Scores
    col("home_score"),
    col("away_score"),
    
    # Stadium
    col("stadium.name").alias("stadium_name"),
    col("stadium.country.name").alias("stadium_country")
).withColumn(
    "winner",
    when(col("home_score") > col("away_score"), "HOME")
    .when(col("away_score") > col("home_score"), "AWAY")
    .otherwise("DRAW")
).withColumn(
    "total_goals",
    col("home_score") + col("away_score")
)

display(matches_clean.limit(10))

# COMMAND ----------

# Write to Snowflake
print("📤 Writing matches to Snowflake...")
matches_clean.write \
    .format("snowflake") \
    .options(**SNOWFLAKE_OPTIONS) \
    .option("dbtable", "FACT_MATCHES") \
    .mode("overwrite") \
    .save()

print("✅ FACT_MATCHES created in Snowflake!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔴 3. Find Manchester United Matches

# COMMAND ----------

# Filter Man United
mu_matches = matches_clean.filter(
    (lower(col("home_team_name")).contains("manchester united")) | 
    (lower(col("away_team_name")).contains("manchester united"))
)

mu_count = mu_matches.count()
print(f"🔴 Manchester United Matches: {mu_count}")

if mu_count > 0:
    display(mu_matches)
    
    # Save to Snowflake
    mu_matches.write \
        .format("snowflake") \
        .options(**SNOWFLAKE_OPTIONS) \
        .option("dbtable", "MANUTD_MATCHES") \
        .mode("overwrite") \
        .save()
    
    print("✅ MANUTD_MATCHES saved!")
else:
    print("⚠️  No direct Man United matches in dataset")
    print("   (StatsBomb Open Data focuses on international & women's competitions)")

# COMMAND ----------

# MAGIC  %md
# MAGIC ## 🎯 4. Process Events (Sample)

# COMMAND ----------

# Get event files with correct path
events_path = f"{DATA_PATH}/events/"
event_files = dbutils.fs.ls(events_path)
total_files = len(event_files)

print(f"📂 Total Event Files: {total_files:,}")

# Process first 100 matches (manageable size)
sample_size = __builtins__.min(100, total_files)
sample_paths = [f.path for f in event_files[:sample_size]]

print(f"📥 Loading {sample_size} matches...")

# Read events with proper options
events_df = (
    spark.read
    .option("multiLine", "true")
    .option("mode", "PERMISSIVE")
    .json(sample_paths)
    .cache()
)

total_events = events_df.count()
print(f"🎯 Total Events: {total_events:,}")



# COMMAND ----------

# Read events with proper options
events_df = (
    spark.read
    .option("multiLine", "true")
    .option("mode", "PERMISSIVE")
    .json(sample_paths)
    .cache()
)

total_events = events_df.count()
print(f"🎯 Total Events: {total_events:,}")

# COMMAND ----------


events_with_match = events_df.withColumn(
    "match_id",
    regexp_extract(input_file_name(), r'events/(\d+)\.json', 1).cast("long")
)

total_events = events_with_match.count()
print(f"🎯 Total Events: {total_events:,}")

# Show sample
print("\n📊 Sample with match_id:")
display(events_with_match.select("match_id", "id", "type", "team", "player").limit(10))

# COMMAND ----------

# Transform events
events_clean = events_with_match.select(
    col("id").alias("event_id"),
    col("match_id"),
    col("index").alias("event_index"),
    col("period"),
    col("timestamp"),
    col("minute"),
    col("second"),
    col("type.id").alias("event_type_id"),
    col("type.name").alias("event_type"),
    col("possession"),
    col("possession_team.id").alias("possession_team_id"),
    col("possession_team.name").alias("possession_team_name"),
    col("play_pattern.name").alias("play_pattern"),
    col("team.id").alias("team_id"),
    col("team.name").alias("team_name"),
    col("player.id").alias("player_id"),
    col("player.name").alias("player_name"),
    col("position.name").alias("position"),
    col("location")[0].alias("location_x"),
    col("location")[1].alias("location_y"),
    col("duration"),
    col("under_pressure").cast("boolean"),
    col("off_camera").cast("boolean")
)

print(f"✅ Transformed {events_clean.count():,} events")
display(events_clean.limit(10))

# COMMAND ----------

# Event type distribution
event_types = events_clean.groupBy("event_type") \
    .count() \
    .orderBy(desc("count"))

print("📊 Top Event Types:")
display(event_types.limit(20))


# COMMAND ----------

# Write sample to Snowflake
print("📤 Writing events sample to Snowflake...")
events_clean.write \
    .format("snowflake") \
    .options(**SNOWFLAKE_OPTIONS) \
    .option("dbtable", "FACT_EVENTS") \
    .mode("overwrite") \
    .save()

print("✅ FACT_EVENTS created in Snowflake!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚽ 5. Extract Passes
# MAGIC

# COMMAND ----------

# Filter passes (use events_with_match instead of events_df)
passes = events_with_match.filter(col("type.name") == "Pass").select(
    col("id").alias("event_id"),
    col("match_id"),  # ← Now exists!
    col("player.name").alias("player_name"),
    col("team.name").alias("team_name"),
    col("minute"),
    col("location")[0].alias("pass_start_x"),
    col("location")[1].alias("pass_start_y"),
    col("pass.end_location")[0].alias("pass_end_x"),
    col("pass.end_location")[1].alias("pass_end_y"),
    col("pass.length").alias("pass_length"),
    col("pass.angle").alias("pass_angle"),
    col("pass.height.name").alias("pass_height"),
    col("pass.body_part.name").alias("pass_body_part"),
    col("pass.outcome.name").alias("pass_outcome"),
    col("pass.recipient.name").alias("pass_recipient"),
    coalesce(col("under_pressure"), lit(False)).alias("under_pressure")
)

pass_count = passes.count()
print(f"⚽ Total Passes: {pass_count:,}")

# COMMAND ----------

# Pass completion by team
pass_stats = passes.groupBy("team_name") \
    .agg(
        count("*").alias("total_passes"),
        sum(when(col("pass_outcome").isNull(), 1).otherwise(0)).alias("completed"),
        sum(when(col("pass_outcome").isNotNull(), 1).otherwise(0)).alias("failed")
    ) \
    .withColumn(
        "completion_rate",
        round((col("completed") / col("total_passes")) * 100, 2)
    ) \
    .orderBy(desc("total_passes"))

display(pass_stats.limit(20))

# COMMAND ----------

# Save to Snowflake
passes.write \
    .format("snowflake") \
    .options(**SNOWFLAKE_OPTIONS) \
    .option("dbtable", "FACT_PASSES") \
    .mode("overwrite") \
    .save()

print("✅ FACT_PASSES saved to Snowflake!")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 6. Extract Shots & xG

# COMMAND ----------

# Filter shots (use events_with_match)
shots = events_with_match.filter(col("type.name") == "Shot").select(
    col("id").alias("event_id"),
    col("match_id"),  # ← Now exists!
    col("player.name").alias("player_name"),
    col("team.name").alias("team_name"),
    col("minute"),
    col("location")[0].alias("shot_x"),
    col("location")[1].alias("shot_y"),
    col("shot.statsbomb_xg").alias("xg"),
    col("shot.outcome.name").alias("shot_outcome"),
    col("shot.type.name").alias("shot_type"),
    col("shot.body_part.name").alias("shot_body_part"),
    col("shot.technique.name").alias("shot_technique"),
    coalesce(col("under_pressure"), lit(False)).alias("under_pressure")
)

shot_count = shots.count()
print(f"🎯 Total Shots: {shot_count:,}")


# COMMAND ----------


# xG by team
team_xg = shots.groupBy("team_name") \
    .agg(
        count("*").alias("total_shots"),
        sum("xg").alias("total_xg"),
        round(avg("xg"), 3).alias("avg_xg_per_shot"),
        sum(when(col("shot_outcome") == "Goal", 1).otherwise(0)).alias("goals")
    ) \
    .withColumn(
        "conversion_rate",
        round((col("goals") / col("total_shots")) * 100, 2)
    ) \
    .orderBy(desc("total_xg"))

print("📊 Expected Goals (xG) by Team:")
display(team_xg.limit(20))

# COMMAND ----------


# Save to Snowflake
shots.write \
    .format("snowflake") \
    .options(**SNOWFLAKE_OPTIONS) \
    .option("dbtable", "FACT_SHOTS") \
    .mode("overwrite") \
    .save()

print("✅ FACT_SHOTS saved to Snowflake!")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 7. Final Summary

# COMMAND ----------

print("="*60)
print("✅ DATA PIPELINE COMPLETE!")
print("="*60)
print()
print("📊 Data Processed:")
print(f"   Competitions: {competitions_df.count()}")
print(f"   Matches: {matches_clean.count():,}")
print(f"   Events (sample): {total_events:,}")
print(f"   Passes: {pass_count:,}")
print(f"   Shots: {shot_count:,}")
print()
print("❄️  Snowflake Tables Created:")
print("   • DIM_COMPETITIONS")
print("   • FACT_MATCHES")
print("   • FACT_EVENTS")
print("   • FACT_PASSES")
print("   • FACT_SHOTS")
if mu_count > 0:
    print("   • MANUTD_MATCHES")
print()
print("🚀 Ready for Analytics & Visualization!")
print("="*60)

# COMMAND ----------

