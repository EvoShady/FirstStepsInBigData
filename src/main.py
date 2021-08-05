import helping_functions as my_project_functions
from my_daframe_api import MyDataframeAPI

"""
Main Program
"""


def main():
    my_project_functions.import_jdbc_dependency()
    spark = my_project_functions.start_spark_session()
    schema = my_project_functions.get_predefined_schema()

    my_df_api = MyDataframeAPI()
    my_df_api.read_data_to_dataframe_from_csv(spark, schema)
    my_df_api.show_dataframe_and_schema()

    my_df_api.group_by_agg_round_avg()
    my_df_api.sort_dataframe()
    my_df_api.show_dataframe_and_schema()

    my_df_api.write_dataframe_to_database(my_project_functions.get_database_proprieties())


if __name__ == '__main__':
    main()
