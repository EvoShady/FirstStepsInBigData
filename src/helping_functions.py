import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

"""
My Project Helping Functions
"""


def start_spark_session():
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName('COVID-19 incidence rate in schools') \
        .config('spark.some.config.option', 'some-value') \
        .getOrCreate()
    return spark


def get_schema():
    schema = StructType([StructField('School Unit Name', StringType(), False),
                         StructField('Elementary School Cases', IntegerType(), True),
                         StructField('Middle School Cases', IntegerType(), True),
                         StructField('High School Cases', IntegerType(), True),
                         StructField('Gender', StringType(), True),
                         StructField('Reporting Date', DateType(), True)])
    return schema


def import_jdbc_dependency():
    findspark.add_packages('mysql:mysql-connector-java:8.0.11')


def get_database_proprieties():
    proprieties = {'user': 'root',
                   'password': 'strongPassword',
                   'driver': 'com.mysql.cj.jdbc.Driver',
                   'useSSL': 'false'}
    return proprieties
