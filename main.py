from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.functions import split, col, size
from pyspark.sql import functions as F
from pyspark.sql.functions import max
from datetime import timedelta, datetime
from google.cloud import bigquery
from google.oauth2 import service_account

# Variables
bucket_files = ["", "", ""]
tempbucket = ""
project_id = ""
# jar_librarie = gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
# region Create Spark Session

spark = SparkSession.builder.getOrCreate()
# endregion


def read_csv(file_path, file):
    """This function reads a csv file from a bucket and creates a dataframe using spark

    Args:
        file_path (string): gs file path where the CSV is located
        file (string): name of the file
    """
    # region Read CSV file and create pyspark data frame
    print(
        "-----------------INFO: ORIGINAL DATA FRAME - "
        + file
        + "---------------------------"
    )

    df_line_items = (
        spark.read.option("inferSchema", "true").option("header", "true").csv(file_path)
    )

    df_line_items.show(10)
    process_df(df_line_items, file)
    # endregion


def process_df(df_raw, file):
    """This function changes the name of the CSV file's columns, as they have spaces on them.

    Args:
        df_raw (dataframe): original dataframe
        file (string): name of the CSV file
    """
    # region Rename columns that have spaces
    newColumns = []
    for field in df_raw.schema.fields:
        new_col = field.name.replace(" ", "_")
        if new_col[0].isdigit() is True:
            new_col = "F_" + new_col
        newColumns.append(new_col)

    oldColumns = df_raw.schema.names

    df_renamed = reduce(
        lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]),
        range(len(oldColumns)),
        df_raw,
    )

    print(
        "-----------------INFO: NEW COLUMN'S NAMES - DATA FRAME - "
        + file
        + " ---------------------------"
    )
    df_renamed.printSchema()
    filter_date(df_renamed, file)

    # endregion


def filter_date(df_renamed, file):
    """As this script is used to load daily information, this function gets the last date where were data from bigquery's
    tables and set that day as last_day_load, depending on the name of the table the query will change and in the case
    of file_3 the function will send the whole dataframe as it does not have a date field to filter.

    Args:
        df_renamed (dataframe): data frame with new names in the columns
        file (_type_): name of the file
    """
    # region #Read data from Big Query to get last data's date

    print("-----------------INFO: CALLING BIQUERY---------------------------")

    client = bigquery.Client(project=project_id)

    if file == "file_1.csv":
        query_job = (
            "SELECT date FROM project_id.dataset.table_1 order by date desc LIMIT 1"
        )
        table_id = "table_1"
    elif file == "file_2.csv":
        query_job = (
            "SELECT date FROM project_id.dataset.table_2 order by date desc LIMIT 1"
        )
        table_id = "table_2"
    elif file == "file_3.csv":
        table_id = "table_3"
        save_df_to_bq(df_renamed, table_id)

    if file != "file_3.csv":
        last_date_load = client.query(query_job).result().to_dataframe().iat[0, 0]
        last_date_load = (
            (datetime.strptime(last_date_load, "%Y-%m-%d") + timedelta(days=1)).date()
        ).strftime("%Y-%m-%d")

        print(
            "-----------------INFO: DATE TO LOAD NEW DATA:"
            + last_date_load
            + "-----------------------"
        )

        # endregion

        # region Filter raw df to just get the last data
        print(
            "-----------------INFO: FILTERING ORIGINAL DATAFRAME - "
            + file
            + "---------------------------"
        )

        df_filter_by_bq_date = df_renamed.filter(df_renamed.Date >= last_date_load)
        df_filter_by_bq_date.show(10, truncate=False)

        save_df_to_bq(df_filter_by_bq_date, table_id)
        # endregion


def save_df_to_bq(df_clean, table_name):
    """This function saves the dataframe in big query using spark, if the table is line_item_lookup_raw,
    it's going to overwrite the existing table.

    Args:
        df_clean (_type_): Clean dataframe filtered
        table_name (_type_): name of the table to save
    """
    # region Save dataframe in BigQuery
    print(
        "-----------------INFO: SAVING DATAFRAME TO BIGQUERY---------------------------"
    )
    table_id = " project_id.dataset." + table_name

    if table_name == "table_3":
        df_clean.write.format("bigquery").mode("overwrite").option(
            "temporaryGcsBucket", tempbucket
        ).option("table", table_id).save()

    df_clean.write.format("bigquery").mode("append").option(
        "temporaryGcsBucket", tempbucket
    ).option("table", table_id).save()
    # endregion


def start_execution():
    """This function executes all the code depending on the files existing in the bucket"""
    for file in bucket_files:
        file_path = "gs://project_id/" + file
        read_csv(file_path, file)


start_execution()
