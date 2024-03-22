from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs,
    avg,
    col,
    max,
    min,
    stddev,
    to_timestamp,
    when,
    coalesce,
    round,
)
from pyspark.sql import Window


def transform(df: DataFrame) -> DataFrame:
    deduped_df = deduplicate(df)
    enriched_df = enrich_metrics(deduped_df)
    imputed_df = impute_missing_values(enriched_df)
    anomalies_df = handle_anomalies(imputed_df)

    return anomalies_df.select(
        to_timestamp("timestamp").alias("timestamp"),
        "turbine_id",
        "wind_speed",
        "wind_direction",
        "power_output",
        "min_power_output",
        "max_power_output",
        "avg_power_output",
        "std_power_output",
        "anomaly_flag_power_output",
        "imputed_flag_power_output",
        "date",
    )


def deduplicate(df: DataFrame) -> DataFrame:
    return df.drop_duplicates(["turbine_id", "timestamp"])


def enrich_metrics(df: DataFrame) -> DataFrame:
    turbine_daily_window = Window.partitionBy(col("turbine_id"), col("date"))
    return (
        df.withColumn(
            "min_power_output", min("power_output").over(turbine_daily_window)
        )
        .withColumn("max_power_output", max("power_output").over(turbine_daily_window))
        .withColumn("avg_power_output", avg("power_output").over(turbine_daily_window))
        .withColumn(
            "std_power_output", stddev("power_output").over(turbine_daily_window)
        )
    )


def impute_missing_values(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "imputed_flag_power_output",
        col("power_output").isNull() & col("avg_power_output").isNotNull(),
    ).withColumn(
        "power_output", round(coalesce(col("power_output"), col("avg_power_output")), 1)
    )


def handle_anomalies(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "anomaly_flag_power_output",
        when(
            abs(col("power_output") - col("avg_power_output"))
            > 2 * col("std_power_output"),
            True,
        ).otherwise(False),
    )
