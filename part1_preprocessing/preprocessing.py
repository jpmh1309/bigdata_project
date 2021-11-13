
import findspark
findspark.init('/usr/lib/python3.7/site-packages/pyspark')

from pyspark.sql.types import (StringType, IntegerType, FloatType, 
                                StructField, StructType)

from pyspark.sql import SparkSession

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
    """

    oscar_df = spark \
        .read \
        .format("csv") \
        .option("header", True) \
        .load(path)

    return oscar_df

def load_imdb_data(spark, path):
    """
    Loads the IMDB data.
    """

    imdb_df = spark \
        .read \
        .format("csv") \
        .option("header", True) \
        .load(path)

    return imdb_df

 def remove_columns(df, columns):
    """
    Removes the specified columns from the dataframe.
    """

    return df.drop(*columns)

def join_dataframes(df1, df2, join_columns):
    """
    Joins the dataframes on the specified columns.
    """

    return df1.join(df2, join_columns)   

def main():
    """
    Main function.
    """

    # Create spark session
    spark = spark_session()

    # Load data
    # oscar_df = spark.read.format("csv").option("header", "true").load("../data/oscar_data.csv")
    # oscar_df = spark \
    #     .read  \
    #     .format("csv")  \
    #     .option("path", "../data/oscar_df.csv") \
    #     .option("header", True) \
    #     .load()
    # Define the schema of the dataframe
    churn_df = spark \
        .read \
        .format("csv") \
        .option("path", "../data/churn_modelling.csv") \
        .option("header", True) \
        .schema(StructType([
                    StructField("RowNumber", IntegerType()),
                    StructField("CustomerId", IntegerType()),
                    StructField("Surname", StringType()),
                    StructField("CreditScore", IntegerType()),
                    StructField("Geography", StringType()),
                    StructField("Gender", StringType()),
                    StructField("Age", IntegerType()),
                    StructField("Tenure", IntegerType()),
                    StructField("Balance", FloatType()),
                    StructField("NumOfProducts", IntegerType()),
                    StructField("HasCrCard", IntegerType()),
                    StructField("IsActiveMember", IntegerType()),
                    StructField("EstimatedSalary", FloatType()),
                    StructField("Exited", IntegerType())])) \
        .load()

    # Print the schema of the dataframe
    churn_df.printSchema()
    churn_df.show()

    # oscar_df.show()
    # oscar_df.printSchema()


if __name__ == "__main__":
    main()