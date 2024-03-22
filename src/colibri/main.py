from pyspark.sql import SparkSession

from config import Config
from pipeline.bronze import BronzePipeline
from pipeline.silver import SilverPipeline

if __name__ == "__main__":
    spark = (
        SparkSession.builder.config("spark.master", "local")
        .appName("colibri")
        .getOrCreate()
    )

    config = Config()

    bronze_pipeline = BronzePipeline(spark, config)
    silver_pipeline = SilverPipeline(spark, config)

    bronze_pipeline.run()
    silver_pipeline.run()
