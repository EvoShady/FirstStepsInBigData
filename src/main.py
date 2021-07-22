from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import IntegerType


def main():
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName('COVID-19 incidence rate in schools') \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # Read from csv file
    data_frame = spark.read.option('multiline', 'true').csv('data/myFile0.csv', header=True, dateFormat='dd-MM-yyyy')
    # Set column types
    data_frame = data_frame.withColumn('Elementary School Cases', data_frame['Elementary School Cases'].cast(IntegerType()))
    data_frame = data_frame.withColumn('Middle School Cases', data_frame['Middle School Cases'].cast(IntegerType()))
    data_frame = data_frame.withColumn('High School Cases', data_frame['High School Cases'].cast(IntegerType()))
    data_frame = data_frame.withColumn('Reporting Date', to_date(data_frame['Reporting Date'], 'dd-MM-yyyy'))
    # Displays the content of the DataFrame to stdout
    data_frame.show()
    data_frame.printSchema()


if __name__ == '__main__':
    main()
