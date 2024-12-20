from chispa.dataframe_comparer import *
from ..jobs.player_player_edges_job import do_player_player_edges_transformation
from collections import namedtuple
GameDetails = namedtuple("GameDetails", "game_id player_id player_name team_abbreviation pts")
PlayerPlayerEdge = namedtuple("PlayerPlayerEdge", "subject_identifier subject_type object_identifier object_type edge_type properties")


def test_player_player_edges(spark):
    source_data = [
        GameDetails(1, 1, 'Kobe Bryant', 'LAK', 10),
        GameDetails(1, 1, 'Kobe Bryant', 'LAK', 20), # Check duplicates removal keeping this record
        GameDetails(1, 2, 'Stephen Curry', 'GSW', 20),
        GameDetails(1, 3, 'Klay Thomson', 'GSW', 20),
        GameDetails(2, 1, 'Kobe Bryant', 'LAK', 50),
        GameDetails(2, 2, 'Stephen Curry', 'GSW', 5),
        GameDetails(2, 3, 'Klay Thomson', 'GSW', 10),
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_player_player_edges_transformation(spark, source_df)
    expected_data = [
        PlayerPlayerEdge(
            subject_identifier=3,
            subject_type='player',
            object_identifier=1,
            object_type='player',
            edge_type='plays_against',
            properties={
                'subject_player_name':'Klay Thomson', # Klay goes first because we compare player_id > player_id
		        'subject_points':'30',
		        'num_games':'2',
		        'object_player_name':'Kobe Bryant',
		        'object_points':'70'
            }
        ),
        PlayerPlayerEdge(
            subject_identifier=3,
            subject_type='player',
            object_identifier=2,
            object_type='player',
            edge_type='shares_team',
            properties={
                'subject_player_name':'Klay Thomson',
		        'subject_points':'30',
		        'num_games':'2',
		        'object_player_name':'Stephen Curry',
		        'object_points':'25'
            }
        ),
        PlayerPlayerEdge(
            subject_identifier=2,
            subject_type='player',
            object_identifier=1,
            object_type='player',
            edge_type='plays_against',
            properties={
                'subject_player_name':'Stephen Curry',
		        'subject_points':'25',
		        'num_games':'2',
		        'object_player_name':'Kobe Bryant',
		        'object_points':'70'
            }
        )
    ]

    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)