import sys
sys.path.insert(0, '../part1_preprocessing')
from preprocessing import extract_year, clean_columns, lower_case_columns, remove_columns, load_oscar_data


def test_read_oscar_data(spark_session):
    """
    Test if the data is read correctly
    """
    actual_df = load_oscar_data(spark_session, "../data/the_oscar_award.csv")
    simpleData =[(1927,
                  1928,
                  1,
                  "ACTOR",
                  "Richard Barthelmess",
                  "The Noose",
                  "False")]

    expected_df = spark_session.createDataFrame(simpleData, 
        ["year_film", 
        "year_ceremony",
        "ceremony", 
        "category", 
        "name", 
        "film", 
        "winner"])

    assert actual_df.first() == expected_df.first()
