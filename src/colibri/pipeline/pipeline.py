from pyspark.sql import DataFrame, SparkSession

from config import Config


class Pipeline:
    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config

    @staticmethod
    def write(
        df: DataFrame, output_path: str
    ):  # Overwrite all existing data in each logical partition for which
        # the write commits new data. Any existing logical partitions for which the write does not contain data
        # remain unchanged.
        df.write.format("parquet").mode("overwrite").partitionBy("date").option(
            "partitionOverwriteMode", "dynamic"
        ).save(output_path)
