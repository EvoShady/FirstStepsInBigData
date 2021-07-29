from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType, StringType, DateType
import findspark


def main():
    findspark.add_packages('mysql:mysql-connector-java:8.0.11')

    spark = start_spark_session()
    schema = get_schema()
    data_frame = spark.read.csv('data/myFile0.csv',
                                header=True,
                                dateFormat='dd-MM-yyyy',
                                schema=schema)

    data_frame.show()
    data_frame.printSchema()

    prop = get_proprieties()
    data_frame.write.jdbc('jdbc:mysql://localhost:3306/covid-19 incidence rate in schools',
                          'test_table_1',
                          'overwrite',
                          prop)


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


def get_proprieties():
    props = {'user': 'root',
             'password': 'strongPassword',
             'driver': 'com.mysql.cj.jdbc.Driver',
             'useSSL': 'false'}
    return props


if __name__ == '__main__':
    main()
