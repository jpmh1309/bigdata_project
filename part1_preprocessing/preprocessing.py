
# import findspark
# findspark.init('/usr/lib/python3.7/site-packages/pyspark')

from pyspark.sql.types import (StringType, IntegerType, FloatType, 
                                StructField, StructType)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,isnan, when, count, lower, to_timestamp, year
from pyspark_dist_explore import hist
import matplotlib.pyplot as plt

def spark_session():
    """
    Creates a SparkSession object.
    """

    spark = SparkSession \
        .builder \
        .appName("Bigdata: Load and Preprocessing") \
        .config("spark.driver.extraClassPath", "postgresql-42.2.14.jar") \
        .config("spark.executor.extraClassPath", "postgresql-42.2.14.jar") \
        .getOrCreate()

    return spark

def load_oscar_data(spark, path):
    """
    Loads the Oscar data.
    :param spark: SparkSession object
    :param path: path to the Oscar data
    """

    # ----------------------------------------------------------------------------- #
    # Features description:
    # - year_film: year of the film (integer)
    # - year_ceremony: year of the ceremony (integer)
    # - ceremony_name: name of the ceremony (string)
    # - category: category of the ceremony (string)
    # - name: name of the film (string)
    # - film: name of the film (string)
    # - winner: winner of the ceremony (string)
    # ----------------------------------------------------------------------------- #

    oscar_df = spark \
        .read \
        .format("csv") \
        .option("path", path) \
        .option("header", True) \
        .schema(StructType([
                    StructField("year_film", IntegerType()),
                    StructField("year_ceremony", IntegerType()),
                    StructField("ceremony", IntegerType()),
                    StructField("category", StringType()),
                    StructField("name", StringType()),
                    StructField("film", StringType()),
                    StructField("winner", StringType())])) \
        .load()

    return oscar_df
                      

def load_imdb_data(spark, path):
    """
    Loads the IMDb data.
    :param spark: SparkSession object
    :param path: path to the IMDb data
    """

    # ----------------------------------------------------------------------------- #
    # Features description:
    # - imdb_title_id: IMDB title id (string)
    # - title: title of the movie (string)
    # - original_title: original title of the movie (string)
    # - year: year of the movie (integer)
    # - date_published: date of publication of the movie (string)
    # - genre: genre of the movie (string)
    # - duration: duration of the movie (integer)
    # - country: country of origin of the movie (string)
    # - language: language of the movie (string)
    # - director: director of the movie (string)
    # - writer: writer of the movie (string)
    # - production_company: production company of the movie (string)
    # - actors: actors of the movie (string)
    # - avg_vote: average vote of the movie (float)
    # - votes: number of votes of the movie (integer)
    # - budget: budget of the movie (float)
    # - usa_gross_income: gross income of the movie in USA (float)
    # - worlwide_gross_income: gross income of the movie in the world (float)
    # - metascore: metascore of the movie (integer)
    # - reviews_from_users: number of reviews from users of the movie (float)
    # - reviews_from_critics: number of reviews from critics of the movie (float)
    # ----------------------------------------------------------------------------- #

    imdb_df = spark \
        .read \
        .format("csv") \
        .option("path", path) \
        .option("header", True) \
        .schema(StructType([
                    StructField("imdb_title_id", StringType()),
                    StructField("title", StringType()),
                    StructField("original_title", StringType()),
                    StructField("year", IntegerType()),
                    StructField("date_published", StringType()),
                    StructField("genre", StringType()),
                    StructField("duration", IntegerType()),
                    StructField("country", StringType()),
                    StructField("language", StringType()),
                    StructField("director", StringType()),
                    StructField("writer", StringType()),
                    StructField("production_company", StringType()),
                    StructField("actors", StringType()),
                    StructField("description", StringType()),
                    StructField("avg_vote", FloatType()),
                    StructField("votes", IntegerType()),
                    StructField("budget", StringType()),
                    StructField("usa_gross_income", StringType()),
                    StructField("worlwide_gross_income", StringType()),
                    StructField("metascore", IntegerType()),
                    StructField("reviews_from_users", FloatType()),
                    StructField("reviews_from_critics", FloatType())])) \
        .load()

    return imdb_df

def load_rotten_tomatoes_data(spark, path):
    """
    Loads the Rotten Tomatoes data.
    :param spark: SparkSession object
    :param path: path to the Rotten Tomatoes data
    """

    # ----------------------------------------------------------------------------- #
    # Features description:
    # - rotten_tomatoes_link: link to the Rotten Tomatoes page of the movie (string)
    # - movie_title: title of the movie (string)
    # - movie_info: information about the movie (string)
    # - critics_consensus: critics consensus about the movie (string)
    # - content_rating: content rating of the movie (string)
    # - genres: list of genres of the movie (string)
    # - directors: list of directors of the movie (string)
    # - authors: list of authors of the movie (string)
    # - actors: list of actors of the movie (string)
    # - original_release_date: original release date of the movie (string)
    # - streaming_release_date: streaming release date of the movie (string)
    # - runtime: runtime of the movie (integer)
    # - production_company: production company of the movie (string)
    # - tomatometer_status: status of the movie on the tomatometer (string)
    # - tomatometer_rating: rating of the movie on the tomatometer (integer)
    # - tomatometer_count: number of votes of the movie on the tomatometer (integer)
    # - audience_status: status of the movie on the audience (string)
    # - audience_rating: rating of the movie on the audience (integer)
    # - audience_count: number of votes of the movie on the audience (integer)
    # - tomatometer_top_critics_count: number of top critics of the movie on the tomatometer (integer)
    # - tomatometer_fresh_critics_count: number of fresh critics of the movie on the tomatometer (integer)
    # - tomatometer_rotten_critics_count: number of rotten critics of the movie on the tomatometer (integer)
    # ----------------------------------------------------------------------------- #

    rotten_tomatoes_df = spark \
        .read \
        .format("csv") \
        .option("path", path) \
        .option("header", True) \
        .schema(StructType([
                    StructField("rotten_tomatoes_link", StringType()),
                    StructField("movie_title", StringType()),
                    StructField("movie_info", StringType()),
                    StructField("critics_consensus", StringType()),
                    StructField("content_rating", StringType()),
                    StructField("genres", StringType()),
                    StructField("directors", StringType()),
                    StructField("authors", StringType()),
                    StructField("actors", StringType()),
                    StructField("original_release_date", StringType()),
                    StructField("streaming_release_date", StringType()),
                    StructField("runtime", IntegerType()),
                    StructField("production_company", StringType()),
                    StructField("tomatometer_status", StringType()),
                    StructField("tomatometer_rating", IntegerType()),
                    StructField("tomatometer_count", IntegerType()),
                    StructField("audience_status", StringType()),
                    StructField("audience_rating", IntegerType()),
                    StructField("audience_count", IntegerType()),
                    StructField("tomatometer_top_critics_count", IntegerType()),
                    StructField("tomatometer_fresh_critics_count", IntegerType()),
                    StructField("tomatometer_rotten_critics_count", IntegerType())])) \
        .load() 

    return rotten_tomatoes_df 

def clean_columns(data):
    """"
    Remove columns with more than 60% of missing values.
    :param data: dataframe
    :return: dataframe
    """
    
    # Remove columns with more than 60% of missing values
    df = data.drop(*[c for c in data.columns if data.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c)]).collect()[0][0] > (data.count() * 0.4)])

    return df

def count_missings(data):
    """
    Counts the number of missing values in each column.
    :param data: dataframe
    :return: dataframe
    """

    # Count the number of missing values in each column
    df = data.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in data.columns])

    return df

# def join_dataframes(df1, df2, join_columns):
#     """
#     Joins the dataframes on the specified columns.
#     """

#     return df1.join(df2, join_columns)   

def lower_case_columns(data, columns):
    """
    Lower cases the columns.
    :param data: dataframe
    :param columns: list of columns to lower case
    :return: dataframe
    """

    # Lower case the columns
    for c in columns:
        data = data.withColumn(c, lower(col(c)))

    return data

def extract_year(data, column):
    """
    Extracts the year from timestamp string and coverts it to integer.
    :return: dataframe
    """

    # Extract the year from timestamp string and coverts it to integer
    data = data.withColumn(column, to_timestamp(col(column)))
    data = data.withColumn(column, year(col(column)))

    return data

def remove_columns(data, columns):
    """
    Removes the columns.
    :param data: dataframe
    :param columns: list of columns to remove
    :return: dataframe
    """

    # Remove the columns
    data = data.drop(*columns)

    return data

def main():
    """
    Main function.
    """

    # Create spark session
    spark = spark_session()

    # Change logging level
    spark.sparkContext.setLogLevel("ERROR")

    # ----------------------------------------------------------------------------- #
    # Load data
    # ----------------------------------------------------------------------------- #

    # Load and clean the Oscar data
    oscar_df = load_oscar_data(spark, "../data/the_oscar_award.csv")
    oscar_df.show()
    oscar_df.printSchema()
    
    # Load and clean the IMDb data
    imdb_df = load_imdb_data(spark, "../data/IMDb movies.csv")
    imdb_df.show()
    imdb_df.printSchema()
    df = count_missings(imdb_df)
    df.show()

    # Load and clean the Rotten Tomatoes data
    rotten_tomatoes_df = load_rotten_tomatoes_data(spark, "../data/rotten_tomatoes_movies.csv")
    rotten_tomatoes_df.show()
    rotten_tomatoes_df.printSchema()
    df = count_missings(rotten_tomatoes_df)
    df.show()

    # ----------------------------------------------------------------------------- #
    # Remove columns that are not useful
    # ----------------------------------------------------------------------------- #

    # In the oscar data, the columns ceremony_name and name are not useful because 
    # they do not contain any valuable information.
    oscar_df = remove_columns(oscar_df, 
        ["ceremony_name", "name"])

    # In the imdb data, the columns imdb_tittle_id, original_title, data_published,
    # country, and language are not useful because they do not contain any valuable
    # information.
    imdb_df = remove_columns(imdb_df, 
        ["imdb_title_id", "original_title", "date_published", "country", "language"])

    # In the rotten tomatoes data, the columns rotten_tomatoes_link, movie_info, 
    # critics_consensus, content_rating, and  streaming_release_date are not useful
    # because they do not contain any valuable information.
    rotten_tomatoes_df = remove_columns(rotten_tomatoes_df, 
        ["rotten_tomatoes_link", "movie_info", "critics_consensus", "content_rating", 
        "streaming_release_date"])


    # ----------------------------------------------------------------------------- #
    # Remove columns with more than 60% of missing values
    # ----------------------------------------------------------------------------- #
    oscar_df = clean_columns(oscar_df)
    imdb_df =  clean_columns(imdb_df) 
    rotten_tomatoes_df = clean_columns(rotten_tomatoes_df)    

    df = count_missings(oscar_df)
    df.show()

    df = count_missings(imdb_df)
    df.show()

    df = count_missings(rotten_tomatoes_df)
    df.show()

    oscar_df.printSchema()
    imdb_df.printSchema()
    rotten_tomatoes_df.printSchema()

    # ----------------------------------------------------------------------------- #
    # Lower case the string columns, this is required for the join to avoid 
    # mismatches due to  the different case of the columns.
    # ----------------------------------------------------------------------------- #
    oscar_df = lower_case_columns(oscar_df, 
        ["category", "film", "winner"])
    imdb_df = lower_case_columns(imdb_df, 
        ["title", "genre", "director", "writer", 
         "production_company", "actors", "description"])
    rotten_tomatoes_df = lower_case_columns(rotten_tomatoes_df,
        ["movie_title", "genres", "directors", "authors", "actors", 
        "original_release_date", "production_company", "tomatometer_status", 
        "audience_status"])

    # ----------------------------------------------------------------------------- #
    # Join the dataframes
    # ----------------------------------------------------------------------------- #

    # Extract the year from timestamp string and coverts it to integer
    rotten_tomatoes_df = extract_year(rotten_tomatoes_df, "original_release_date")

    rotten_tomatoes_df.printSchema()
    rotten_tomatoes_df.show()

    # Join rotten tomatoes data with imdb data
    cond = [imdb_df.title.contains(rotten_tomatoes_df.movie_title), imdb_df.year == rotten_tomatoes_df.original_release_date]

    imdb_rt_join = imdb_df.join(rotten_tomatoes_df, cond, how='inner')

    imdb_rt_join.printSchema()

    # Get number of rows
    print("Number of rows: {}".format(imdb_rt_join.count()))

    # Join rotten tomatoes + imdb data with oscar data
    cond = [imdb_rt_join.title.contains(oscar_df.film), imdb_rt_join.year == oscar_df.year_film]

    df_join = imdb_rt_join.join(oscar_df, cond, how='inner')

    df_join.printSchema()
    df_join.show()

    # Get number of rows
    print("Number of rows: {}".format(df_join.count()))

    df_join.coalesce(1).write.mode('overwrite').option("header", True).csv("../data/join")

    # ----------------------------------------------------------------------------- #
    # Remove duplicated columns
    # ----------------------------------------------------------------------------- #

    # ----------------------------------------------------------------------------- #
    # TODO: Convert string columns to integer columns
    # ----------------------------------------------------------------------------- #
    
    # ----------------------------------------------------------------------------- #
    # Impute missing values
    # ----------------------------------------------------------------------------- #

    # ----------------------------------------------------------------------------- #
    # TODO: Graphs and statistics
    # ----------------------------------------------------------------------------- #
    df_join = df_join.withColumn('winner',
                                 when(df_join.winner == 'true', 1)
                                .when(df_join.winner == 'false', 0)
                                .otherwise(df_join.winner))
    df_join = df_join .withColumn('winner', df_join['winner'].cast(IntegerType()))

    # Plot histogram of awards column
    df_join.select("winner").show()

    fig, ax = plt.subplots(nrows=2, ncols=2)
    fig.set_size_inches(20, 20)

    hist(ax[0, 0], df_join.select('winner'))
    ax[0, 0].set_title('winner')

    plt.show()

if __name__ == "__main__":
    main()