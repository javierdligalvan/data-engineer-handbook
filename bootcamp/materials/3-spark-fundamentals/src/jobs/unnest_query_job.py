from pyspark.sql import SparkSession

# Convert flattened table with map season_stats back into initial table with more records
query = """

WITH unnested AS (
  SELECT
    player_name,
    season_stats_exploded
  FROM players
  LATERAL VIEW EXPLODE(season_stats) AS season_stats_exploded
  WHERE player_name = 'Michael Jordan'
)

SELECT
  player_name,
  season_stats_exploded.season AS season,
  season_stats_exploded.gp AS gp,
  season_stats_exploded.pts AS pts,
  season_stats_exploded.reb AS reb,
  season_stats_exploded.ast AS ast
FROM unnested;

"""


def do_unnest_query_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("players")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("unnest_query") \
      .getOrCreate()
    output_df = do_unnest_query_transformation(spark, spark.table("players"))
    output_df.write.mode("overwrite").insertInto("players_unnested")

