from pyspark.sql import SparkSession
from app.service.service_handler import ServiceHandler


def get_spark_obj():
    """
    Function to build Spark Session
    :return: Spark Session Object
    """
    partitions = 8
    shuffle_partitions = 200
    master = "local[*]"
    spark = SparkSession.builder \
        .appName("time_series_data") \
        .master(master) \
        .config("spark.sql.execution.arrow.enabled", "true") \
        .config("spark.default.parallelism", str(partitions)) \
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions)) \
        .getOrCreate()
    return spark


def trigger():
    """
    Main function
    """
    spark_obj = get_spark_obj()
    input_filepath = "/home/Desktop/Raw data/*.csv"
    output_filepath = "/home/Desktop/output"
    input_data_df = spark_obj.read.format("csv") \
        .load(input_filepath).toDF("location_index", "device_id", "lat", "lng",
                                   "date", "carrier", "timestamp", "pincode",
                                   "city")
    select_data_df = input_data_df.select("location_index", "device_id", "timestamp")
    if input_data_df.head(1):
        ServiceHandler.process_data(spark_obj, select_data_df, output_filepath)
    spark_obj.stop()


if __name__ == "__main__":
    trigger()
