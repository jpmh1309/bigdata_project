
from pyspark.sql import SparkSession

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
    """
    Main function.
    """

    spark = spark_session()


if __name__ == "__main__":
    main()