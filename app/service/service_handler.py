from pyspark.sql.functions import udf, col, unix_timestamp, lead, lit, explode, count, countDistinct, from_unixtime, \
    round
from pyspark.sql.types import *
from pyspark.sql.window import Window


class ServiceHandler:

    @staticmethod
    def map_func(row):
        """
        Function to map row with device_id and indexed_ts
        """
        return (row["device_id"], row["indexed_ts"]), row

    @staticmethod
    def reduce_func(x, y):
        """
        Given two objects, returns the one with latest timestamp
        """
        if x["timestamp"] > y["timestamp"]:
            return x
        return y

    @staticmethod
    def range_func(start, end, max_range):
        """
        Returns sequence of integers from start to end
        """
        if end:
            return list(range(start, end))
        else:
            return list(range(start, max_range))

    @classmethod
    def process_data(cls, spark, input_data_df, output_filepath, partitions=8):
        """
        Function to process time series data and compute average number of devices (rounded off to 4 decimal digits)
        within a location index in every 10 minute window
        """

        input_data_df = input_data_df.withColumn("timestamp", unix_timestamp(col("timestamp")).cast("int")) \
            .withColumn("indexed_ts", (col("timestamp") / 600).cast("int"))
        input_data_df = input_data_df.rdd.map(cls.map_func).reduceByKey(cls.reduce_func).toDF().select("_2.*")
        input_data_df = input_data_df.withColumn("next_location_ts", lead("indexed_ts").over(
            Window.partitionBy("device_id").orderBy("indexed_ts")))
        # Finding maximum indexed timestamp
        max_ts = input_data_df.select("indexed_ts").groupBy().max("indexed_ts").collect()[0].asDict()['max(indexed_ts)']

        get_range = udf(cls.range_func, ArrayType(IntegerType()))
        ranged_ts_df = input_data_df.withColumn("ts_range",
                                                get_range(col("indexed_ts"), col("next_location_ts"), lit(max_ts))) \
            .drop("timestamp", "indexed_ts", "next_location_ts")

        # Exploding the dataframe to create rows for timestamps between recorded data
        exploded_df = ranged_ts_df.withColumn("timestamp", explode(col("ts_range"))).drop("ts_range") \
            .repartition(partitions, "timestamp")

        grouped_data_df = exploded_df.groupBy("timestamp") \
            .agg(count("device_id").alias("devices_count"), countDistinct("location_index").alias("locations_count"))
        final_data_df = grouped_data_df.withColumn("timestamp", from_unixtime(col("timestamp") * 600)) \
            .withColumn("average_devices_", round(col("devices_count") / col("locations_count"), 4)) \
            .drop("devices_count", "locations_count").orderBy(col("timestamp").desc())
        # Writing output dataframe onto disk in csv format
        final_data_df.repartition(1).write.mode('overwrite').csv(output_filepath, header=True)
