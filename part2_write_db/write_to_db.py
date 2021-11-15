
import findspark
findspark.init('/usr/lib/python3.7/site-packages/pyspark')

from pyspark.ml.feature import StandardScaler, StringIndexer, OneHotEncoder, VectorAssembler, Imputer
from pyspark.sql.functions import explode, array, lit, col
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline


import sys
sys.path.insert(0, '../part1_preprocessing')
from preprocessing import extract_year, clean_columns, lower_case_columns, remove_columns, load_rotten_tomatoes_data, load_imdb_data, load_oscar_data

def spark_session():
    """
    Creates a SparkSession object.
    """

    spark = SparkSession \
        .builder \
        .appName("Bigdata: Write to DB") \
        .config("spark.driver.extraClassPath", "postgresql-42.2.14.jar") \
        .config("spark.executor.extraClassPath", "postgresql-42.2.14.jar") \
        .getOrCreate()

    return spark


def writeToDB(data, table_name):
    """
    Write data to database.
    :param data: data to be written to database
    :param table_name: name of table to write to
    :return:
    """
    data \
        .write \
        .format("jdbc") \
        .mode('overwrite') \
        .option("url", "jdbc:postgresql://172.17.0.1:5433/postgres") \
        .option("user", "postgres") \
        .option("password", "testPassword") \
        .option("dbtable", table_name) \
        .save()


def main():

    # Create SparkSession object
    spark = spark_session()

    # Change logging level
    spark.sparkContext.setLogLevel("ERROR")

    # ----------------------------------------------------------------------------- #
    # Write to DB data without preprocessing
    # ----------------------------------------------------------------------------- #
    
    # Load and clean the Oscar data
    # Features description:
    # - year_film: year of the film (integer)
    # - year_ceremony: year of the ceremony (integer)
    # - ceremony_name: name of the ceremony (string)
    # - category: category of the ceremony (string)
    # - name: name of the film (string)
    # - film: name of the film (string)
    # - winner: winner of the ceremony (string)
    oscar_df = load_oscar_data(spark, "../data/the_oscar_award.csv")
    oscar_df.show()
    oscar_df.printSchema()
    
    # Load and clean the IMDb data
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
    imdb_df = load_imdb_data(spark, "../data/IMDb_movies.csv")
    imdb_df.show()
    imdb_df.printSchema()

    # Load and clean the Rotten Tomatoes data
    # Features description:
    # - rotten_tomatoes_link: link to the Rotten Tomatoes page of the movie (string)
    # - movie_title: title of the movie (string)
    # - movie_info: information about the movie (string)
    # - critics_consensus: critics consensus about the movie (string)
    # - content_rating: content rating of the movie (string)
    # - genres: list of genres of the movie (string)
    # - directors: list of directors of the movie (string)
    # - authors: list of authors of the movie (string)
    # - actors_rt: list of actors of the movie (string)
    # - original_release_date: original release date of the movie (string)
    # - streaming_release_date: streaming release date of the movie (string)
    # - runtime: runtime of the movie (integer)
    # - production_company_rt: production company of the movie (string)
    # - tomatometer_status: status of the movie on the tomatometer (string)
    # - tomatometer_rating: rating of the movie on the tomatometer (integer)
    # - tomatometer_count: number of votes of the movie on the tomatometer (integer)
    # - audience_status: status of the movie on the audience (string)
    # - audience_rating: rating of the movie on the audience (integer)
    # - audience_count: number of votes of the movie on the audience (integer)
    # - tomatometer_top_critics_count: number of top critics of the movie on the tomatometer (integer)
    # - tomatometer_fresh_critics_count: number of fresh critics of the movie on the tomatometer (integer)
    # - tomatometer_rotten_critics_count: number of rotten critics of the movie on the tomatometer (integer)
    rotten_tomatoes_df = load_rotten_tomatoes_data(spark, "../data/rotten_tomatoes_movies.csv")
    rotten_tomatoes_df.show()
    rotten_tomatoes_df.printSchema()

    # Write the data to the database
    writeToDB(oscar_df, "oscar_award")
    writeToDB(imdb_df, "imdb_movies")
    writeToDB(rotten_tomatoes_df, "rotten_tomatoes_movies")

    # ----------------------------------------------------------------------------- #
    # Preprocess the data
    # ----------------------------------------------------------------------------- #

    # Remove columns that are not useful
    # ----------------------------------------------------------------------------- #
    # In the oscar data, the columns year_ceremony, ceremony_name, ceremony, and 
    # name are not useful because  they do not contain any valuable information.
    oscar_df = remove_columns(oscar_df, 
        ["ceremony_name", "ceremony", "year_ceremony", "name"])

    # In the imdb data, the columns imdb_tittle_id, original_title, data_published,
    # country, description, and language are not useful because they do not contain 
    # any valuable information.
    imdb_df = remove_columns(imdb_df, 
        ["description", "imdb_title_id", "original_title", "date_published", "country", "language"])

    # In the rotten tomatoes data, the columns rotten_tomatoes_link, movie_info, 
    # critics_consensus, content_rating, and  streaming_release_date, audience_status 
    # are not useful because they do not contain any valuable information.
    rotten_tomatoes_df = remove_columns(rotten_tomatoes_df, 
        ["rotten_tomatoes_link", "movie_info", "critics_consensus", "content_rating", 
        "streaming_release_date", "audience_status", "tomatometer_status"])

    # Remove columns with more than 60% of missing values
    # ----------------------------------------------------------------------------- #
    oscar_df = clean_columns(oscar_df)
    imdb_df =  clean_columns(imdb_df) 
    rotten_tomatoes_df = clean_columns(rotten_tomatoes_df)    
    
    # Lower case the string columns, this is required for the join to avoid 
    # mismatches due to  the different case of the columns.
    # ----------------------------------------------------------------------------- #
   
    oscar_df = lower_case_columns(oscar_df, 
        ["category", "film", "winner"])
    imdb_df = lower_case_columns(imdb_df, 
        ["title", "genre", "director", "writer", 
         "production_company", "actors"])
    rotten_tomatoes_df = lower_case_columns(rotten_tomatoes_df,
        ["movie_title", "genres", "directors", "authors", "actors_rt", 
        "original_release_date", "production_company_rt"])

    # Join the dataframes
    # ----------------------------------------------------------------------------- #

    # Extract the year from timestamp string and coverts it to integer
    rotten_tomatoes_df = extract_year(rotten_tomatoes_df, "original_release_date")

    # Join rotten tomatoes data with imdb data
    cond = [imdb_df.title == rotten_tomatoes_df.movie_title, imdb_df.year == rotten_tomatoes_df.original_release_date]
    imdb_rt_join = imdb_df.join(rotten_tomatoes_df, cond, how='inner')

    # Join rotten tomatoes + imdb data with oscar data
    cond = [imdb_rt_join.title == oscar_df.film, imdb_rt_join.year == oscar_df.year_film]
    df_join = imdb_rt_join.join(oscar_df, cond, how='inner')

    # Remove duplicated columns
    # ----------------------------------------------------------------------------- #
    df_join = remove_columns(df_join, 
        ["movie_title", "film", "genres", "actors_rt", "directors", "year_film", 
         "original_release_date", "runtime", "authors", "production_company_rt"])


    # Convert string columns to integer columns
    # ----------------------------------------------------------------------------- #
   
    categorical_columns = ["genre", "director", "writer", "production_company", "actors", "category",  "winner"]

    # The index of string vlaues multiple columns
    indexers = [
        StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c))
        for c in categorical_columns
    ]

    # The encode of indexed vlaues multiple columns
    encoders = [OneHotEncoder(dropLast=False,inputCol=indexer.getOutputCol(),
                outputCol="{0}_encoded".format(indexer.getOutputCol())) 
        for indexer in indexers
    ]

    # Vectorizing encoded values
    assembler = VectorAssembler(inputCols=[encoder.getOutputCol() for encoder in encoders],outputCol="features")

    pipeline = Pipeline(stages=indexers + encoders+[assembler])
    model=pipeline.fit(df_join)

    df_ohe= model.transform(df_join)

    # Impute missing values
    # ----------------------------------------------------------------------------- #
    
    columns_features = ["duration", "director_indexed", "writer_indexed", 
                "production_company_indexed", "actors_indexed", "genre_indexed", 
                "avg_vote", "votes", "reviews_from_users", "reviews_from_critics", 
                "tomatometer_rating", "tomatometer_count", "audience_rating", 
                "audience_count", "tomatometer_top_critics_count", 
                "tomatometer_fresh_critics_count", "tomatometer_rotten_critics_count"]
    
    target = ["winner_indexed"]
    
    columns_kept = columns_features + target

    df_ohe= df_ohe.select(columns_kept)

    imputer = Imputer()

    imputer.setInputCols(columns_features)
    imputer.setOutputCols(columns_features)
    imputer.getRelativeError()

    model = imputer.fit(df_ohe)
    df_imputer = model.transform(df_ohe)

    # Oversampling  
    # ----------------------------------------------------------------------------- #

    df_imputer.groupby("winner_indexed").count().show()

    major_df = df_imputer.filter(df_imputer.winner_indexed == 0.0)
    minor_df = df_imputer.filter(df_imputer.winner_indexed == 1.0)
    ratio = int(major_df.count()/minor_df.count())
    print("ratio: {}".format(ratio))

    a = range(ratio)

    # Duplicate the minority rows
    df_tmp = minor_df.withColumn("dummy", explode(array([lit(x) for x in a]))).drop('dummy')

    # Combine both oversampled minority rows and previous majority rows 
    df_oversampled = major_df.unionAll(df_tmp)

    # Normalization Scaling
    # ----------------------------------------------------------------------------- #
    
    assembler = VectorAssembler(
        inputCols=columns_features,
        outputCol='Features')

    df_vector = assembler.transform(df_oversampled)
    df_vector = df_vector.select(['Features', 'winner_indexed'])
    df_vector.show()

    standard_scaler = StandardScaler(inputCol='Features', outputCol='scaledFeatures')
    scale_model = standard_scaler.fit(df_vector)

    df_scaled = scale_model.transform(df_vector)
    df_scaled = df_scaled.select(['scaledFeatures', 'winner_indexed'])

    # ----------------------------------------------------------------------------- #
    # Write the data to the database
    # ----------------------------------------------------------------------------- #
    
    # Transform to RDD 
    # ----------------------------------------------------------------------------- #
    df_clean = (df_scaled.withColumn("scaledFeatures", vector_to_array("scaledFeatures"))).select(["winner_indexed"] + [col("scaledFeatures")[i].alias(columns_features[i]) for i in range(len(columns_features))])
    df_clean.printSchema()
    df_clean.show()

    # Save to DB
    # ----------------------------------------------------------------------------- #
    # Feature description:
    # - winner_indexed: 0 = winner, 1 = not winner (double)
    # - duration: duration of the film in minutes (double)
    # - director_indexed: index of the director (double)
    # - writer_indexed: index of the writer (double)
    # - production_company_indexed: index of the production company (double)
    # - actors_indexed: index of the actors (double)
    # - genre_indexed: index of the genre (double)
    # - avg_vote: average vote of the film (double)
    # - votes: number of votes of the film  (double)
    # - reviews_from_users: number of reviews from users (double)
    # - reviews_from_critics: number of reviews from critics (double)
    # - tomatometer_rating: rating of the film by the tomatometer (double)
    # - tomatometer_count: number of ratings of the film by the tomatometer
    # - audience_rating: rating of the film by the audience (double)
    # - audience_count: number of ratings of the film by the audience
    # - tomatometer_top_critics_count: number of top critics ratings of the film by 
    #                                  the tomatometer (double)
    # - tomatometer_fresh_critics_count: number of fresh critics ratings of the film 
    #                                    by the tomatometer (double)
    # - tomatometer_rotten_critics_count: number of rotten critics ratings of the 
    #                                     film by the tomatometer (double)
    # ----------------------------------------------------------------------------- #

    df_clean \
        .write \
        .format("jdbc") \
        .mode('overwrite') \
        .option("url", "jdbc:postgresql://172.17.0.1:5433/postgres") \
        .option("user", "postgres") \
        .option("password", "testPassword") \
        .option("dbtable", "df_clean") \
        .save()

if __name__ == "__main__":
    main()