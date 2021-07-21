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
    df = spark.read.option('multiline', 'true').csv('data/myFile0.csv', header=True, dateFormat='dd-MM-yyyy')
    # Set column types
    df = df.withColumn('Elementary School Cases', df['Elementary School Cases'].cast(IntegerType()))
    df = df.withColumn('Middle School Cases', df['Middle School Cases'].cast(IntegerType()))
    df = df.withColumn('High School Cases', df['High School Cases'].cast(IntegerType()))
    df = df.withColumn('Reporting Date', to_date(df['Reporting Date'], 'dd-MM-yyyy'))
    # Displays the content of the DataFrame to stdout
    df.show()
    df.printSchema()


if __name__ == '__main__':
    main()
