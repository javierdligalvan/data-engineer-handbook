from pyspark.sql import SparkSession

query = """

WITH deduped AS (
    SELECT *, row_number() over (PARTITION BY player_id, game_id ORDER BY pts DESC) AS row_num
    FROM game_details
),
    filtered AS (
        SELECT * FROM deduped
        WHERE row_num = 1
    ),
    aggregated AS (
        SELECT
            f1.player_id AS subject_player_id,
            f1.player_name AS subject_player_name,
            f2.player_id AS object_player_id,
            f2.player_name AS object_player_name,
            CASE WHEN f1.team_abbreviation = f2.team_abbreviation
                THEN 'shares_team'
            ELSE 'plays_against'
            END  AS edge_type,
            COUNT(1) AS num_games,
            SUM(f1.pts) AS subject_points,
            SUM(f2.pts) as object_points
        FROM filtered f1
        JOIN filtered f2
            ON f1.game_id = f2.game_id
            AND f1.player_name <> f2.player_name
        WHERE f1.player_id > f2.player_id
        GROUP BY
            subject_player_id,
            subject_player_name,
            object_player_id,
            object_player_name,
            CASE WHEN f1.team_abbreviation = f2.team_abbreviation
                THEN 'shares_team'
            ELSE 'plays_against'
            END
    )

select
	subject_player_id as subject_identifier,
	'player' as subject_type,
	object_player_id as object_identifier,
	'player' as object_type,
	edge_type as edge_type,
	map(
		'subject_player_name', subject_player_name,
		'subject_points', subject_points,
		'num_games', num_games,
		'object_player_name',  object_player_name,
		'object_points', object_points
	) as properties
from aggregated;

"""


def do_player_player_edges_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("player_player_edge") \
      .getOrCreate()
    output_df = do_player_player_edges_transformation(spark, spark.table("game_details"))
    output_df.write.mode("overwrite").insertInto("game_details")

