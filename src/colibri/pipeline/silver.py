from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import abs, avg, col, max, min, stddev, to_timestamp, when

from config import Config
from pyspark.sql import Window


class SilverPipeline:
    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config

    def run(self):
        df = self.read(self.spark, self.config.bronze_path)
        transformed_df = self.transform(df)
        self.write(transformed_df, self.config.silver_path)

    @staticmethod
    def read(spark: SparkSession, input_path: str):
        return spark.read.format("parquet").load(input_path).drop("input_file_name")

    @staticmethod
    def transform(df: DataFrame) -> DataFrame:
        # dedupe

        # remove/impute missing values/outliers
        # interpolate missing values? and missing rows

        # append in Bronze, delta table as is (partition on date? (assume data is large))
        # transformed data into silver (read entire partition to interpolate)
        # argue for separation of apps to run on different schedules
        # think about backfill

        # enrich
        turbine_daily_window = Window.partitionBy(col("turbine_id"), col("date"))
        enriched_df = df.withColumn("min_power_output", min("power_output").over(turbine_daily_window)) \
            .withColumn("max_power_output", max("power_output").over(turbine_daily_window)) \
            .withColumn("avg_power_output", avg("power_output").over(turbine_daily_window)) \
            .withColumn("std_power_output", stddev("power_output").over(turbine_daily_window)) \
            .withColumn("anomaly_power_output", when(
            abs(col("power_output")) > col("avg_power_output") + 2 * col("std_power_output"), True)
                        .otherwise(False)
                        )
        return enriched_df.withColumn("timestamp", to_timestamp("timestamp"))

    @staticmethod
    def write(df: DataFrame, output_path: str):
        # late arriving data, need to merge instead of overwrite
        df.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(output_path)
