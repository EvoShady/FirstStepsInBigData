import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from src.my_daframe_api import MyDataframeAPI
import mysql.connector

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


def get_predefined_schema():
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


def remake_initial_table_to_db(spark, path, table_name):
    my_df = MyDataframeAPI()
    my_df.read_data_to_dataframe_from_csv(spark, path, get_predefined_schema())
    my_df.group_by_agg_round_avg()
    my_df.sort_dataframe()
    my_df.write_dataframe_to_database(table_name, get_database_proprieties())


def drop_table(table_name):
    conn = mysql.connector.connect(
        user='root', password='strongPassword', host='localhost', database='covid-19 incidence rate in schools')
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
    conn.close()


def rename_table(table_name, new_table_name):
    conn = mysql.connector.connect(
        user='root', password='strongPassword', host='localhost', database='covid-19 incidence rate in schools')
    cursor = conn.cursor()
    cursor.execute(f'RENAME TABLE {table_name} to {new_table_name}')
    conn.close()
