import pytest

from src.colibri.transform.silver import transform


class TestTransform:
    @pytest.fixture(scope="session")
    def test_app(self, spark):
        input_data = [
            ("2022-03-01 00:00:00", 1, 11.8, 169, 2.7),
            ("2022-03-01 00:00:00", 1, 11.8, 169, 2.7), # dupe
            ("2022-03-01 02:00:00", 1, 13.8, 73, None), # missing value
            ("2022-03-01 03:00:00", 1, 10.5, 61, 0.8), # outlier
            ("2022-03-01 04:00:00", 1, 9.1, 209, 2.3),
            ("2022-03-01 05:00:00", 1, 12.4, 116, 2.2),
            ("2022-03-01 06:00:00", 1, 9.2, 97, 4.2),
            ("2022-03-01 07:00:00", 1, 10.1, 87, 4.0),
            ("2022-03-01 08:00:00", 1, 12.6, 150, 1.6),
            ("2022-03-01 09:00:00", 1, 9.1, 31, 2.7),
            ("2022-03-01 10:00:00", 1, 9.6, 127, 2.8),
            ("2022-03-01 11:00:00", 1, 12.9, 348, 4.0),
            ("2022-03-01 12:00:00", 1, 11.9, 93, 2.0),
            ("2022-03-01 13:00:00", 1, 11.4, 289, 3.9),
            ("2022-03-01 14:00:00", 1, 9.4, 316, 3.9),
            ("2022-03-01 15:00:00", 1, 9.7, 135, 2.9),
            ("2022-03-01 16:00:00", 1, 12.4, 289, 2.3),
            ("2022-03-01 17:00:00", 1, 11.7, 253, 4.3),
            ("2022-03-01 18:00:00", 1, 10.1, 240, 1.8),
            ("2022-03-01 19:00:00", 1, 14.7, 131, 2.5),
            ("2022-03-01 20:00:00", 1, 9.2, 300, 3.1),
            ("2022-03-01 21:00:00", 1, 10.8, 328, 2.5),
            ("2022-03-01 22:00:00", 1, 13.1, 286, 2.2),
            ("2022-03-01 23:00:00", 1, 13.0, 52, 4.4),
            # turbine 2
            ("2022-03-01 00:00:00", 2, 11.6, 24, 2.2),
            ("2022-03-01 01:00:00", 2, 12.8, 35, 4.2),
            # other date
            ("2022-03-02 00:00:00", 1, 10.7, 101, 3.4),
            ("2022-03-02 01:00:00", 1, 14.1, 272, 2.1),
            ("2022-03-02 00:00:00", 2, 10.7, 233, 1.7),
            ("2022-03-02 01:00:00", 2, 13.5, 313, 2.4)
        ]

        expected_data = [
            ("2022-03-01 00:00:00", 1, 11.8, 169, 2.7),
            ("2022-03-01 01:00:00", 1, 11.6, 152, 4.4),
            ("2022-03-01 02:00:00", 1, 13.8, 73, 2.9), # outlier
            ("2022-03-01 03:00:00", 1, 10.5, 61, None), # missing value
            ("2022-03-01 00:00:00", 2, 11.6, 24, 2.2),
            ("2022-03-01 01:00:00", 2, 12.8, 35, 4.2),
            ("2022-03-02 00:00:00", 1, 10.7, 101, 3.4),
            ("2022-03-02 00:00:00", 2, 10.7, 233, 1.7),
        ]

        input_column_names = ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output", "date"]
        expected_column_names = ["timestamp",
                                 "turbine_id",
                                 "wind_speed",
                                 "wind_direction",
                                 "power_output",
                                 "min_power_output",
                                 "max_power_output",
                                 "avg_power_output",
                                 "std_power_output",
                                 "anomaly_flag_power_output",
                                 "date"
                                 ]
        input_df = spark.createDataFrame(input_data, input_column_names)
        expected_df = spark.createDataFrame(expected_data, expected_column_names)

        actual_df = transform(input_df)

        actual_df.collect() == expected_df.collect()
