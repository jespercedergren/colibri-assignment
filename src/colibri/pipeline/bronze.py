from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import input_file_name, to_date

from pipeline.pipeline import Pipeline
from config import Config


class BronzePipeline(Pipeline):
    def __init__(self, spark: SparkSession, config: Config):
        super().__init__(spark, config)

    def run(self):
        df = self.read(self.config.raw_input_path)
        enriched_df = self.enrich(df)
        self.write(enriched_df, self.config.bronze_path)

    def read(self, input_path: str) -> DataFrame:
        return (
            self.spark.read.option("inferSchema", True)
            .option("header", True)
            .csv(input_path)
        )

    @staticmethod
    def enrich(df: DataFrame) -> DataFrame:
        return df.withColumn("date", to_date("timestamp")).withColumn(
            "input_file_name", input_file_name()
        )
