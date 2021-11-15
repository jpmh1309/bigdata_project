import sys
sys.path.insert(0, '../part1_preprocessing')
from preprocessing import extract_year, clean_columns, lower_case_columns, remove_columns, load_rotten_tomatoes_data


def test_read_rotten_data(spark_session):
    """
    Test if the data is read correctly
    """
    actual_df = load_rotten_tomatoes_data(spark_session, "../data/rotten_tomatoes_movies.csv")
    
    simpleData =[( 
        "m/0814255",
        "Percy Jackson & the Olympians: The Lightning Thief",
        "Always trouble-prone, the life of teenager Percy Jackson (Logan Lerman) gets a lot more complicated when he learns he's the son of the Greek god Poseidon. At a training ground for the children of deities, Percy learns to harness his divine powers and prepare for the adventure of a lifetime: he must prevent a feud among the Olympians from erupting into a devastating war on Earth, and rescue his mother from the clutches of Hades, god of the underworld.",
        "Though it may seem like just another Harry Potter knockoff, Percy Jackson benefits from a strong supporting cast, a speedy plot, and plenty of fun with Greek mythology.",
        "PG",
        "Action & Adventure, Comedy, Drama, Science Fiction & Fantasy",
        "Chris Columbus",
        "Craig Titley, Chris Columbus, Rick Riordan",
        "Logan Lerman, Brandon T. Jackson, Alexandra Daddario, Jake Abel, Sean Bean, Pierce Brosnan, Steve Coogan, Rosario Dawson, Melina Kanakaredes, Catherine Keener, Kevin Mckidd, Joe Pantoliano, Uma Thurman, Ray Winstone, Julian Richings, Bonita Friedericy, Annie Ilonzeh, Tania Saulnier, Marie Avgeropoulos, Luisa D'Oliveira, Christie Laing, Marielle Jaffe, Elisa King, Chrystal Tisiga, Alexis Knapp, Charlie Gallant, Chelan Simmons, Andrea Brooks, Natassia Malthe, Max Van Ville, Serinda Swan, Dimitri Lekkos, Ona Grauer, Stefanie von Pfetten, Conrad Coates, Erica Cerra, Dylan Neal, Luke Camilleri, Holly Hougham, Ina Geraldine, Raquel Riskin, Yusleidis Oquendo, Janine Edwards, Valerie Tian, Violet Columbus, Sarah Smyth, Merritt Patterson, Julie Luck, Andrea Day, John Stewart, Suzanne Ristic, Deejay Jackson, Matthew Garrick, Stan Carp, Suzanna Ristic, Richard Harmon, Maria Olsen, Robin Lemon, Doyle Devereux, Tom Pickett, VJ Delos-Reyes, Tim Aas, Keith Dallas, Spencer Atkinson, Maya Washington, Loyd Bateman, Victor Ayala, Zane Holtz, Eli Zagoudakis, Matt Reimer, Rob Hayter, Lloyd Bateman, Shawn Beaton, Jarod Joseph, Reilly Dolman, Paul Cummings, Julie Brar, Dejan Loyola, Damian Arman, Mario Casoria, Dorla Bell, Carolyn Adair (II), Jade Pawluk, G. Patrick Currie, Darian Arman, Mariela Zapata, David L. Smith",
        "2010-02-12",
        "2015-11-25",
        119,
        "20th Century Fox",
        "Rotten",
        49,
        149,
        "Spilled",
        53,
        254421,
        43,
        73,
        76)]

    expected_df = spark_session.createDataFrame(simpleData, 
        ["rotten_tomatoes_link", 
         "movie_title", 
         "movie_info", 
         "critics_consensus", 
         "content_rating", 
         "genres", 
         "directors", 
         "authors", 
         "actors_rt", 
         "original_release_date", 
         "streaming_release_date", 
         "runtime", 
         "production_company_rt", 
         "tomatometer_status", 
         "tomatometer_rating", 
         "tomatometer_count", 
         "audience_status", 
         "audience_rating", 
         "audience_count", 
         "tomatometer_top_critics_count", 
         "tomatometer_fresh_critics_count", 
         "tomatometer_rotten_critics_count"])
   
    assert actual_df.first() == expected_df.first()


def test_imdb_data_remove_empty_columns(spark_session):
    """
    Test that ensures that we remove columns that are null in 60% of the data
    """
    rotten_df = load_rotten_tomatoes_data(spark_session, "../data/rotten_tomatoes_movies.csv")

    # Remove columns that are null in 60% of the data
    actual_df = clean_columns(rotten_df)

    # Expected dataframe should be the same as the actual dataframe
    # since there are no columns that are null in 60% of the data
    expected_df = clean_columns(actual_df)


    assert actual_df.collect() == expected_df.collect()


def test_remove_colums_rotten_data(spark_session):
    """
    Test if the data is read correctly
    """
    actual_df = load_rotten_tomatoes_data(spark_session, "../data/rotten_tomatoes_movies.csv")
    simpleData =[( 
        "m/0814255",
        "Percy Jackson & the Olympians: The Lightning Thief",
        "Always trouble-prone, the life of teenager Percy Jackson (Logan Lerman) gets a lot more complicated when he learns he's the son of the Greek god Poseidon. At a training ground for the children of deities, Percy learns to harness his divine powers and prepare for the adventure of a lifetime: he must prevent a feud among the Olympians from erupting into a devastating war on Earth, and rescue his mother from the clutches of Hades, god of the underworld.",
        "Though it may seem like just another Harry Potter knockoff, Percy Jackson benefits from a strong supporting cast, a speedy plot, and plenty of fun with Greek mythology.",
        "PG",
        "Action & Adventure, Comedy, Drama, Science Fiction & Fantasy",
        "Chris Columbus",
        "Craig Titley, Chris Columbus, Rick Riordan",
        "Logan Lerman, Brandon T. Jackson, Alexandra Daddario, Jake Abel, Sean Bean, Pierce Brosnan, Steve Coogan, Rosario Dawson, Melina Kanakaredes, Catherine Keener, Kevin Mckidd, Joe Pantoliano, Uma Thurman, Ray Winstone, Julian Richings, Bonita Friedericy, Annie Ilonzeh, Tania Saulnier, Marie Avgeropoulos, Luisa D'Oliveira, Christie Laing, Marielle Jaffe, Elisa King, Chrystal Tisiga, Alexis Knapp, Charlie Gallant, Chelan Simmons, Andrea Brooks, Natassia Malthe, Max Van Ville, Serinda Swan, Dimitri Lekkos, Ona Grauer, Stefanie von Pfetten, Conrad Coates, Erica Cerra, Dylan Neal, Luke Camilleri, Holly Hougham, Ina Geraldine, Raquel Riskin, Yusleidis Oquendo, Janine Edwards, Valerie Tian, Violet Columbus, Sarah Smyth, Merritt Patterson, Julie Luck, Andrea Day, John Stewart, Suzanne Ristic, Deejay Jackson, Matthew Garrick, Stan Carp, Suzanna Ristic, Richard Harmon, Maria Olsen, Robin Lemon, Doyle Devereux, Tom Pickett, VJ Delos-Reyes, Tim Aas, Keith Dallas, Spencer Atkinson, Maya Washington, Loyd Bateman, Victor Ayala, Zane Holtz, Eli Zagoudakis, Matt Reimer, Rob Hayter, Lloyd Bateman, Shawn Beaton, Jarod Joseph, Reilly Dolman, Paul Cummings, Julie Brar, Dejan Loyola, Damian Arman, Mario Casoria, Dorla Bell, Carolyn Adair (II), Jade Pawluk, G. Patrick Currie, Darian Arman, Mariela Zapata, David L. Smith",
        "2010-02-12",
        "2015-11-25",
        119,
        "20th Century Fox",
        "Rotten",
        49,
        149,
        "Spilled",
        53,
        254421,
        43)]

    expected_df = spark_session.createDataFrame(simpleData, 
        ["rotten_tomatoes_link", 
         "movie_title", 
         "movie_info", 
         "critics_consensus", 
         "content_rating", 
         "genres", 
         "directors", 
         "authors", 
         "actors_rt", 
         "original_release_date", 
         "streaming_release_date", 
         "runtime", 
         "production_company_rt", 
         "tomatometer_status", 
         "tomatometer_rating", 
         "tomatometer_count", 
         "audience_status", 
         "audience_rating", 
         "audience_count", 
         "tomatometer_top_critics_count"])

    actual_df = remove_columns(actual_df, 
        ["tomatometer_fresh_critics_count", 
         "tomatometer_rotten_critics_count"])




    assert actual_df.first() == expected_df.first()