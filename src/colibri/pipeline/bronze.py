from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import input_file_name, to_date

from config import Config


class BronzePipeline:
    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config

    def run(self):
        df = self.read(self.spark, self.config.raw_input_path)
        enriched_df = self.enrich(df)
        self.write(enriched_df, self.config.bronze_path)

    @staticmethod
    def read(spark: SparkSession, input_path: str) -> DataFrame:
        return spark.read.option("inferSchema", True).option("header", True).csv(input_path)

    @staticmethod
    def enrich(df: DataFrame) -> DataFrame:
        return df.withColumn("date", to_date("timestamp")).withColumn("input_file_name", input_file_name())

    @staticmethod
    def write(df: DataFrame, output_path: str):  # Overwrite all existing data in each logical partition for which
        # the write commits new data. Any existing logical partitions for which the write does not contain data
        # remain unchanged.
        df.write.format("parquet").mode("overwrite").partitionBy("date").option("partitionOverwriteMode", "dynamic").save(output_path)
