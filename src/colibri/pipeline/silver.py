from pyspark.sql import SparkSession

from pipeline.pipeline import Pipeline
from config import Config
from transform.silver import transform


class SilverPipeline(Pipeline):
    def __init__(self, spark: SparkSession, config: Config):
        super().__init__(spark, config)

    def run(self):
        df = self.read(self.config.bronze_path)
        transformed_df = transform(df)
        self.write(transformed_df, self.config.silver_path)

    def read(self, input_path: str):
        return (
            self.spark.read.format("parquet").load(input_path).drop("input_file_name")
        )
