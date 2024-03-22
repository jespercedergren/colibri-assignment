import pytest

from src.colibri.config import Config
from src.colibri.pipeline.bronze import BronzePipeline
from src.colibri.pipeline.silver import SilverPipeline


class E2ETest:
    @pytest.mark.skip(reason="Not implemented yet")
    def test_app(self, spark):
        config = Config()

        bronze_pipeline = BronzePipeline(spark, config)
        silver_pipeline = SilverPipeline(spark, config)

        bronze_pipeline.run()
        silver_pipeline.run()

        # assert output
