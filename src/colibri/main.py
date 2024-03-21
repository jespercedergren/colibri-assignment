from pyspark.sql import SparkSession

from config import Config
from pipeline.bronze import BronzePipeline
from pipeline.silver import SilverPipeline

if __name__ == "__main__":
    print("Running main")
    spark = SparkSession.builder.config("spark.master", "local").appName("colibri").getOrCreate()

    print("Spark initialized")
    config = Config()

    bronze_pipeline = BronzePipeline(spark, config)
    silver_pipeline = SilverPipeline(spark, config)

    bronze_pipeline.run()
    silver_pipeline.run()


# plot power over time (for each turbine)
#   with interval
# plot daily average over time (day)
# plot moving average (daily over time)

# impute with
# a) some kind of average,
# b) or linear interpolation between missing values

# c) or average seasonality for hour
# d) or linear interpolation using hour seasonality

# does wind speed and wind_direction help with power output and

# create imputed value columns

