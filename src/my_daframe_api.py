from pyspark.sql.functions import avg, round

"""
My Dataframe API Class
"""


class MyDataframeAPI:

    def __init__(self):
        self.dataframe = ''

    def read_data_to_dataframe_from_csv(self, spark, schema):
        self.dataframe = spark.read.csv(path='../data/covid19_data.csv',
                                        header=True,
                                        dateFormat='dd-MM-yyyy',
                                        schema=schema)

    def show_dataframe_and_schema(self):
        self.dataframe.show()
        self.dataframe.printSchema()

    def group_by_agg_round_avg(self):
        self.dataframe = self.dataframe.groupBy('School Unit Name', 'Gender').agg(
            round(avg('Elementary School Cases'), 4),
            round(avg('Middle School Cases'), 4),
            round(avg('High School Cases'), 4))

    def sort_dataframe(self):
        self.dataframe = self.dataframe.sort('School Unit Name')

    def write_dataframe_to_database(self, database_proprieties):
        self.dataframe.write.jdbc(url='jdbc:mysql://localhost:3306/covid-19 incidence rate in schools',
                                  table='table_after_transformations',
                                  mode='overwrite',
                                  properties=database_proprieties)
