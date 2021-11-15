import sys
import pytest
sys.path.insert(0, '../part1_preprocessing')
from preprocessing import extract_year, clean_columns, lower_case_columns, remove_columns, load_imdb_data


# @pytest.mark.skip(reason="FIXME: This test is not working yet")
def test_read_imdb_data(spark_session):
    """
    Test if the data is read correctly
    """
    actual_df = load_imdb_data(spark_session, "../data/IMDb_movies.csv")

    simpleData =[("tt0000009",
                  "Miss Jerry",
                  "Miss Jerry",
                  1894,
                  "1894-10-09",
                  "Romance",
                  45,
                  "USA",
                  str(),
                  "Alexander Black",
                  "Alexander Black",
                  "Alexander Black Photoplays",
                  "Blanche Bayliss, William Courtenay", 
                  "Chauncey Depew",
                  "The adventures of a female reporter in the 1890s.",
                  5.9,
                  154,
                  str(),
                  str(),
                  str(),
                  int(),
                  1.0,
                  2.0)]

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
