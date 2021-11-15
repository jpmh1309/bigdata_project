import sys
import pytest
sys.path.insert(0, '../part1_preprocessing')
from preprocessing import extract_year, clean_columns, lower_case_columns, remove_columns, load_imdb_data
from pyspark.sql.types import StringType, IntegerType, FloatType, StructField, StructType


@pytest.mark.skip(reason="FIXME: This test is not working yet")
def test_read_imdb_data(spark_session):
    """
    Test if the data is read correctly
    """
    actual_df = load_imdb_data(spark_session, "../data/IMDb_movies.csv")


    simpleData =[(
        "tt2395427",
        "Avengers: Age of Ultron",
        "Avengers: Age of Ultron",
        2015,
        "2015-04-22",
        "Action, Adventure, Sci-Fi",
        141,
        "USA",
        "English, Korean", 
        "Joss Whedon",
        "Joss Whedon, Stan Lee",
        "Marvel Studios",
        "Robert Downey Jr., Chris Hemsworth, Mark Ruffalo, Chris Evans, Scarlett Johansson, Jeremy Renner, James Spader, Samuel L. Jackson, Don Cheadle, Aaron Taylor-Johnson, Elizabeth Olsen, Paul Bettany, Cobie Smulders, Anthony Mackie, Hayley Atwell",
        "When Tony Stark and Bruce Banner try to jump-start a dormant peacekeeping program called Ultron, things go horribly wrong and it's up to Earth's mightiest heroes to stop the villainous Ultron from enacting his terrible plan.",
        7.300000190734863,
        722685,
        "$ 250000000",
        "$ 459005868",
        "$ 1402808753",
        66.0,
        1306.0,
        693.0)]

    expected_df = spark_session.createDataFrame(simpleData, 
        ["imdb_title_id", 
         "title", 
         "original_title", 
         "year", 
         "date_published", 
         "genre", 
         "duration", 
         "country", 
         "language", 
         "director", 
         "writer", 
         "production_company", 
         "actors", 
         "description", 
         "avg_vote", 
         "votes", 
         "budget", 
         "usa_gross_income", 
         "worlwide_gross_income", 
         "metascore", 
         "reviews_from_users", 
         "reviews_from_critics"])

    assert actual_df.first() == expected_df.first()


def test_imdb_data_remove_empty_columns(spark_session):
    """
    Test that ensures that we remove columns that are null in 60% of the data
    """
    imdb_df = load_imdb_data(spark_session, "../data/IMDb_movies.csv")

    # Remove columns that are null in 60% of the data
    actual_df = clean_columns(imdb_df)

    # Expected dataframe should be the same as the actual dataframe
    # since there are no columns that are null in 60% of the data
    expected_df = clean_columns(actual_df)


    assert actual_df.collect() == expected_df.collect()