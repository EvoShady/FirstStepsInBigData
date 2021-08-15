import helping_functions as hf
from my_daframe_api import MyDataframeAPI

"""
Main Program
"""

PATH_CSV_1 = '../data/covid19_data.csv'
PATH_CSV_2 = '../data/covid19_data_2.csv'
TABLE_NAME = 'table_after_transformations'
DUMMY_TABLE_NAME = 'dummy_table_after_transformations'


def main():
    hf.import_jdbc_dependency()
    spark = hf.start_spark_session()

    hf.remake_initial_table_to_db(spark, PATH_CSV_1, TABLE_NAME)

    df_api_1 = MyDataframeAPI()
    df_api_1.read_dataframe_from_database(spark, TABLE_NAME, hf.get_database_proprieties())
    df_api_1.group_by_agg_round_avg()
    df_api_1.sort_dataframe()
    df_api_1.show_dataframe_and_schema()

    df_api_2 = MyDataframeAPI()
    df_api_2.read_data_to_dataframe_from_csv(spark, PATH_CSV_2, hf.get_predefined_schema())
    df_api_2.group_by_agg_round_avg()
    df_api_2.sort_dataframe()
    df_api_2.show_dataframe_and_schema()

    df_api_1.union_dataframes(df_api_2.dataframe)
    df_api_1.group_by_agg_round_avg()
    df_api_1.sort_dataframe()
    df_api_1.show_dataframe_and_schema()
    # workaround
    df_api_1.write_dataframe_to_database(DUMMY_TABLE_NAME, hf.get_database_proprieties())

    df_api_1.read_dataframe_from_database(spark, DUMMY_TABLE_NAME, hf.get_database_proprieties())
    df_api_1.write_dataframe_to_database(TABLE_NAME, hf.get_database_proprieties())
    hf.drop_dummy_table(DUMMY_TABLE_NAME)


if __name__ == '__main__':
    main()
