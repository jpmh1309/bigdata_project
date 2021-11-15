
# import findspark
# findspark.init('/usr/lib/python3.7/site-packages/pyspark')

from pyspark.sql.functions import col, isnan, when, count, lower, to_timestamp, year, concat_ws, size, explode, array, lit
from pyspark.ml.feature import Tokenizer, StringIndexer, OneHotEncoder, VectorAssembler, Imputer, StandardScaler, StopWordsRemover
from pyspark.sql.types import StringType, IntegerType, FloatType, StructField, StructType
from pyspark.ml.stat import Correlation
from pyspark.sql import SparkSession
from pyspark_dist_explore import hist
import matplotlib.pyplot as plt
from pyspark.ml import Pipeline
import seaborn as sns

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
                    StructField("actors_rt", StringType()),
                    StructField("original_release_date", StringType()),
                    StructField("streaming_release_date", StringType()),
                    StructField("runtime", IntegerType()),
                    StructField("production_company_rt", StringType()),
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

def tokenize_column(data, columns):
    """
    Tokenizes the column.
    :param data: dataframe
    :param column: column to tokenize
    :return: dataframe
    """

    # Tokenize the column
    for column in columns:
        data = data.filter(col(column).isNotNull())
        tokenizer = Tokenizer(inputCol=column, outputCol=column+"_tokens")
        data = tokenizer.transform(data)

    return data

def remove_stop_words(data, columns):
    """
    Removes the stop words from the column.
    :param data: dataframe
    :param column: column to remove stop words
    :return: dataframe
    """

    # Remove the stop words from the column
    for column in columns:
        remover = StopWordsRemover()
        remover.setInputCol(column)
        remover.setOutputCol(column+"_no_stopw")
        data = remover.transform(data)

    return data

def concatenate_columns(data, columns):
    """
    Concatenates the columns.
    :param data: dataframe
    :param columns: list of columns to concatenate
    :return: dataframe
    """

    # Concatenate the columns
    for column in columns:
        data = data.filter(col(column).isNotNull())
        data = data.withColumn(column, concat_ws(", ", col(column)))

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
    imdb_df = load_imdb_data(spark, "../data/IMDb_movies.csv")
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
         "production_company", "actors"])
    rotten_tomatoes_df = lower_case_columns(rotten_tomatoes_df,
        ["movie_title", "genres", "directors", "authors", "actors_rt", 
        "original_release_date", "production_company_rt"])

    # # ----------------------------------------------------------------------------- #
    # # Tokenize the string columns
    # # ----------------------------------------------------------------------------- #
    # oscar_df = tokenize_column(oscar_df, ["film"])
    # imdb_df = tokenize_column(imdb_df, ["title"])
    # rotten_tomatoes_df = tokenize_column(rotten_tomatoes_df, ["movie_title"])

    # # ----------------------------------------------------------------------------- #
    # # Remove stop words
    # # ----------------------------------------------------------------------------- #
    # oscar_df = remove_stop_words(oscar_df, ["film_tokens"])
    # imdb_df = remove_stop_words(imdb_df, ["title_tokens"])
    # rotten_tomatoes_df = remove_stop_words(rotten_tomatoes_df, ["movie_title_tokens"])

    # # oscar_df.printSchema()
    # # imdb_df.printSchema()
    # # rotten_tomatoes_df.printSchema()

    # # ----------------------------------------------------------------------------- #
    # # Concatenate string arrays columns to array
    # # ----------------------------------------------------------------------------- #
    # oscar_df = concatenate_columns(oscar_df, ["film_tokens_no_stopw"])
    # imdb_df = concatenate_columns(imdb_df, ["title_tokens_no_stopw"])
    # rotten_tomatoes_df = concatenate_columns(rotten_tomatoes_df, ["movie_title_tokens_no_stopw"])

    # ----------------------------------------------------------------------------- #
    # Remove temporary columns
    # ----------------------------------------------------------------------------- #
    # oscar_df = remove_columns(oscar_df, ["film_tokens"])
    # imdb_df = remove_columns(imdb_df, ["title_tokens"])
    # rotten_tomatoes_df = remove_columns(rotten_tomatoes_df, ["movie_title_tokens"])

    # Print tokens and no stop words columns

    oscar_df.printSchema()
    imdb_df.printSchema()
    rotten_tomatoes_df.printSchema()

    # oscar_df.select("film_tokens_no_stopw").show()
    # imdb_df.select("title_tokens_no_stopw").show()
    # rotten_tomatoes_df.select("movie_title_tokens_no_stopw").show()

    # ----------------------------------------------------------------------------- #
    # Join the dataframes
    # ----------------------------------------------------------------------------- #

    # Extract the year from timestamp string and coverts it to integer
    rotten_tomatoes_df = extract_year(rotten_tomatoes_df, "original_release_date")

    rotten_tomatoes_df.printSchema()
    rotten_tomatoes_df.show()

    # Join rotten tomatoes data with imdb data
    # cond = [imdb_df.title.contains(rotten_tomatoes_df.movie_title), imdb_df.year == rotten_tomatoes_df.original_release_date]
    cond = [imdb_df.title == rotten_tomatoes_df.movie_title, imdb_df.year == rotten_tomatoes_df.original_release_date]
    # cond = [imdb_df.title_tokens.contains(rotten_tomatoes_df.movie_title_tokens), imdb_df.year == rotten_tomatoes_df.original_release_date]
    # cond = [imdb_df.title_tokens_no_stopw == rotten_tomatoes_df.movie_title_tokens_no_stopw, imdb_df.year == rotten_tomatoes_df.original_release_date]

    imdb_rt_join = imdb_df.join(rotten_tomatoes_df, cond, how='inner')

    imdb_rt_join.printSchema()
    imdb_rt_join.show()

    # Get number of rows
    print("Number of rows: {}".format(imdb_rt_join.count()))

    # Join rotten tomatoes + imdb data with oscar data
    cond = [imdb_rt_join.title.contains(oscar_df.film), imdb_rt_join.year == oscar_df.year_film]
    cond = [imdb_rt_join.title == oscar_df.film, imdb_rt_join.year == oscar_df.year_film]
    # cond = [imdb_rt_join.title_tokens.contains(oscar_df.film_tokens), imdb_rt_join.year == oscar_df.year_film]
    # cond = [imdb_rt_join.title_tokens_no_stopw == oscar_df.film_tokens_no_stopw, imdb_rt_join.year == oscar_df.year_film]


    df_join = imdb_rt_join.join(oscar_df, cond, how='inner')

    df_join.printSchema()
    df_join.show()

    # Get number of rows
    print("Number of rows: {}".format(df_join.count()))

    # ----------------------------------------------------------------------------- #
    # Remove temporary columns
    # ----------------------------------------------------------------------------- #
    # df_join = remove_columns(df_join, [
    #     "film_tokens", "title_tokens", "movie_title_tokens",
    #     "film_tokens_no_stopw", "title_tokens_no_stopw", "movie_title_tokens_no_stopw"])

    # ----------------------------------------------------------------------------- #
    # Remove duplicated columns
    # ----------------------------------------------------------------------------- #

    # title == movie_title and film
    # genre == genres
    # actor == actors_rt
    # director == directors
    # production_company == production_company_rt
    # year == year_film and original_release_date
    # duration == runtime
    # writer == authors

    df_join = remove_columns(df_join, 
        ["movie_title", "film", "genres", "actors_rt", "directors", "year_film", 
         "original_release_date", "runtime", "authors", "production_company_rt"])

    # ----------------------------------------------------------------------------- #
    # Convert string columns to integer columns
    # ----------------------------------------------------------------------------- #

    df_join.printSchema()

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
    df_ohe.printSchema()
    df_ohe.show()

    # ----------------------------------------------------------------------------- #
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

    df_imputer.printSchema()
    df_imputer.show()

    # df_imputer = remove_columns(df_imputer, 
    #     ["genre_indexed_encoded", "director_indexed_encoded", "writer_indexed_encoded", 
    #      "production_company_indexed_encoded", "actors_indexed_encoded", 
    #      "category_indexed_encoded",  "winner_indexed_encoded"])

    # df = count_missings(df_imputer)
    # df.show()

    # ----------------------------------------------------------------------------- #
    # TODO: Graphs and statistics
    # ----------------------------------------------------------------------------- #

    df_imputer.describe(['duration', 'director_indexed', 'writer_indexed']).show()
    df_imputer.describe(['production_company_indexed', 'actors_indexed', 'genre_indexed']).show()
    df_imputer.describe(['avg_vote', 'votes', 'reviews_from_users']).show()
    df_imputer.describe(['reviews_from_critics', 'tomatometer_rating', 'tomatometer_count']).show()
    df_imputer.describe(['audience_rating', 'audience_count', 'tomatometer_top_critics_count']).show()
    df_imputer.describe(['tomatometer_fresh_critics_count', 'tomatometer_rotten_critics_count', 'winner_indexed']).show()

    # Plot histogram of awards column
    df.select("genre_indexed").show()

    # Plot histogram of awards column
    df.select("winner_indexed").show()

    df_join = df   

    fig, ax = plt.subplots(nrows=4, ncols=2)
    fig.set_size_inches(20, 20)

    hist(ax[0, 0], df.select('duration'))
    ax[0, 0].set_title('duration')

    hist(ax[0, 1], df.select('director_indexed'))
    ax[0, 1].set_title('director_indexed')

    hist(ax[1, 0], df.select('writer_indexed'))
    ax[1, 0].set_title('writer_indexed')

    hist(ax[1, 1], df.select('production_company_indexed'))
    ax[1, 1].set_title('production_company_indexed')

    hist(ax[2, 0], df.select('actors_indexed'))
    ax[2, 0].set_title('actors_indexed')

    hist(ax[2, 1], df.select('avg_vote'))
    ax[2, 1].set_title('avg_vote')

    hist(ax[3, 0], df.select('votes'))
    ax[3, 0].set_title('votes')

    hist(ax[3, 1], df.select('reviews_from_users'))
    ax[3, 1].set_title('reviews_from_users')

    plt.show()

    fig, ax = plt.subplots(nrows=4, ncols=2)
    fig.set_size_inches(20, 20)

    hist(ax[4, 0], df.select('reviews_from_critics'))
    ax[0, 0].set_title('reviews_from_critics')

    hist(ax[4, 1], df.select('tomatometer_rating'))
    ax[0, 1].set_title('tomatometer_rating')

    hist(ax[5, 0], df.select('tomatometer_count'))
    ax[1, 0].set_title('tomatometer_count')

    hist(ax[5, 1], df.select('audience_rating'))
    ax[1, 1].set_title('audience_rating')

    hist(ax[6, 0], df.select('audience_count'))
    ax[3, 0].set_title('audience_count')

    hist(ax[6, 1], df.select('tomatometer_top_critics_count'))
    ax[3, 1].set_title('tomatometer_top_critics_count')

    plt.show()

    fig, ax = plt.subplots(nrows=2, ncols=2)
    fig.set_size_inches(20, 20)

    hist(ax[7, 0], df.select('tomatometer_fresh_critics_count'))
    ax[0, 0].set_title('tomatometer_fresh_critics_count')

    hist(ax[7, 1], df.select('tomatometer_rotten_critics_count'))
    ax[0, 1].set_title('tomatometer_rotten_critics_count')

    hist(ax[8, 0], df.select('winner_indexed'))
    ax[1, 0].set_title('winner_indexed')
   
    hist(ax[8, 1], df.select('genre_indexed'))
    ax[1, 1].set_title('genre_indexed')

    plt.show()

    # ----------------------------------------------------------------------------- #
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

    df_oversampled.groupby("winner_indexed").count().show()

    # ----------------------------------------------------------------------------- #
    # Correlation Matrix
    # ----------------------------------------------------------------------------- #
    assembler = VectorAssembler(
        inputCols=columns_features,
        outputCol='Features')

    df_vector = assembler.transform(df_oversampled)
    df_vector = df_vector.select(['Features', 'winner_indexed'])
    df_vector.show()

    # Con la representación de vectores podemos calcular correlaciones
    pearson_matrix = Correlation.corr(df_vector, 'Features').collect()[0][0]
    sns.heatmap(pearson_matrix.toArray(), annot=True, fmt=".2f", cmap='RdYlGn', xticklabels=columns_kept, yticklabels=columns_kept)
    plt.show()

    # ----------------------------------------------------------------------------- #
    # Normalization Scaling
    # ----------------------------------------------------------------------------- #
    standard_scaler = StandardScaler(inputCol='Features', outputCol='scaledFeatures')
    scale_model = standard_scaler.fit(df_vector)

    df_scaled = scale_model.transform(df_vector)
    df_scaled = df_scaled.select(['scaledFeatures', 'winner_indexed'])
    df_scaled.printSchema()
    df_scaled.show()

if __name__ == "__main__":
    main()