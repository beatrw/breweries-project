from pyspark.sql.functions import count
from pyspark.sql import DataFrame
from functools import reduce
from datetime import datetime

from breweries_plugin.gcs_buckets import GcsBuckets
from breweries_plugin.logging_config import configure_logging
from breweries_plugin.spark_config import create_spark_session


class DataRulesInjector:
    """Class responsible for ingesting business rules
    and saving it to GCS Buckets
    """

    def __init__(self):

        self.log = configure_logging()

    def execute(self) -> None:
        """Execute the flow order of functions"""

        self.bucket_name = "storage-breweries"
        self.origin_layer_name = "silver"
        self.destination_layer_name = "gold"

        self.spark = create_spark_session('business_task')
        self.gcs_buckets = GcsBuckets()

        df = self._read_from_gcs()

        df_agregated = self._agregate_data(df)

        self._save_on_gcs(df_agregated)

        self._post_processing()


    def _read_from_gcs(self) -> DataFrame:
        """Reads all data from the current month

        Returns:
            df (DataFrame): Data in a pyspark DataFrame
        """

        self.log.info("Reading data from GCS...")

        blob_path = self.gcs_buckets.get_path_in_layer(self.bucket_name,
                                                        self.origin_layer_name)

        df = self.spark.read.parquet(blob_path)

        return df

    def _agregate_data(self, df: DataFrame) -> DataFrame:
        """Aggregates the data by location(country, state) and brewery type

        Args:
            df (DataFrame): Pyspark dataframe to be aggregated

        Returns:
            aggregated_df (DataFrame): The aggregated data
        """

        aggregated_df = df.groupBy("country", "state", "brewery_type") \
                                    .agg(count("*").alias("brewery_count"))

        aggregated_df.show(truncate=False)

        return aggregated_df

    def _save_on_gcs(self, df: DataFrame) -> None:
        """Saves the transformed data to a GCS bucket in the gold layer
        at the current month path

        Args:
            df (DataFrame): The transformed data to be saved
        """

        blob_path = self.gcs_buckets.get_path_in_layer(self.bucket_name,
                                                        self.destination_layer_name)

        self.log.info(f"Saving data to GCS: {blob_path}")

        df.write.mode("overwrite").parquet(blob_path)

    def _post_processing(self) -> None:
        """Post processing step to be executed after the transformation"""

        self.spark.stop()

        self.log.info("Transform task completed successfully")