from chispa.dataframe_comparer import *
from ..jobs.unnest_query_job import do_unnest_query_transformation
from collections import namedtuple
PlayersFlattened = namedtuple("PlayersFlattened", "player_name season_stats")
PlayerFiltered = namedtuple("PlayerFiltered", "player_name season gp pts reb ast")

def test_unnest_query(spark):
    source_data = [
        PlayersFlattened(player_name='Michael Jordan',
                         season_stats=[
                             {
                                 'season':1996,
                                 'gp':80,
                                 'pts':30,
                                 'reb':6,
                                 'ast':5
                             },
                             {
                                 'season':1997,
                                 'gp':78,
                                 'pts':50,
                                 'reb':8,
                                 'ast':8
                             },
                             {
                                 'season':2001,
                                 'gp':70,
                                 'pts':40,
                                 'reb':10,
                                 'ast':10
                             }
        ]),
        PlayersFlattened(player_name='Ray Allen',
                         season_stats=[
                             {
                                 'season':1996,
                                 'gp':80,
                                 'pts':30,
                                 'reb':6,
                                 'ast':5
                             },
                             {
                                 'season':1997,
                                 'gp':78,
                                 'pts':50,
                                 'reb':8,
                                 'ast':8
                             },
                             {
                                 'season':2001,
                                 'gp':70,
                                 'pts':40,
                                 'reb':10,
                                 'ast':10
                             }
        ]),
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_unnest_query_transformation(spark, source_df)
    expected_data = [
        PlayerFiltered(
            player_name='Michael Jordan',
            season=1996,
            gp=80,
            pts=30,
            reb=6,
            ast=5
        ),
        PlayerFiltered(
            player_name='Michael Jordan',
            season=1997,
            gp=78,
            pts=50,
            reb=8,
            ast=8
        ),
        PlayerFiltered(
            player_name='Michael Jordan',
            season=2001,
            gp=70,
            pts=40,
            reb=10,
            ast=10
        )
    ]

    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)