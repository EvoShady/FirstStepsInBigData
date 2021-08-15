from pyspark.sql.functions import avg, round

"""
My Dataframe API Class
"""


class MyDataframeAPI:

    def __init__(self):
        self.dataframe = ''

    def read_data_to_dataframe_from_csv(self, spark, path, schema):
        self.dataframe = spark.read.csv(path=path,
                                        header=True,
                                        dateFormat='dd-MM-yyyy',
                                        schema=schema)

    def show_dataframe_and_schema(self):
        self.dataframe.show()
        self.dataframe.printSchema()

    def group_by_agg_round_avg(self):
        self.dataframe = self.dataframe.groupBy('School Unit Name', 'Gender').agg(
            round(avg('Elementary School Cases'), 2).alias('Elementary School Cases'),
            round(avg('Middle School Cases'), 2).alias('Middle School Cases'),
            round(avg('High School Cases'), 2).alias('High School Cases'))

    def sort_dataframe(self):
        self.dataframe = self.dataframe.sort('School Unit Name')

    def write_dataframe_to_database(self, table_name, database_proprieties):
        self.dataframe.write.jdbc(url='jdbc:mysql://localhost:3306/covid-19 incidence rate in schools',
                                  table=table_name,
                                  mode='overwrite',
                                  properties=database_proprieties)

    def read_dataframe_from_database(self, spark, table_name, database_proprieties):
        self.dataframe = spark.read.jdbc(url='jdbc:mysql://localhost:3306/covid-19 incidence rate in schools',
                                         table=table_name,
                                         properties=database_proprieties)

    def union_dataframes(self, dataframe):
        self.dataframe = self.dataframe.union(dataframe)
