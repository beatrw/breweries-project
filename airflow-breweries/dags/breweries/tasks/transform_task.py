from pyspark.sql.functions import lit
from pyspark.sql import DataFrame
from functools import reduce
from datetime import datetime

from breweries_plugin.gcs_buckets import GcsBuckets
from breweries_plugin.logging_config import configure_logging
from breweries_plugin.spark_config import create_spark_session
from breweries.tools.breweries_schema import SchemaTables


class DataTrasformer:
    """Class responsible for transforming raw data
    from openbrewerydb API and saving it to GCS Buckets
    in a format ready for analysis
    """

    def __init__(self):

        self.log = configure_logging()

    def execute(self) -> None:
        """Execute the flow order of functions"""

        self.bucket_name = "storage-breweries"
        self.origin_layer_name = "bronze"
        self.destination_layer_name = "silver"

        self.spark = create_spark_session('transform_task')
        self.gcs_buckets = GcsBuckets()

        df_raw = self._read_from_gcs()

        df_transformed = self._transform(df_raw)

        self._save_on_gcs(df_transformed)

        self._post_processing()


    def _read_from_gcs(self) -> DataFrame:
        """Reads all files from the current month

        Returns:
            df (DataFrame): The extracted data in a DataFrame, concatenate all
            files of the current month
        """

        self.log.info("Reading data from GCS...")

        blobs_list = self.gcs_buckets.get_file_list_in_layer(self.bucket_name,
                                                            self.origin_layer_name)

        dfs_list = [self.spark.read.option("multiline", "true").json(blob) \
                    for blob in blobs_list]

        df = reduce(lambda df1, df2: df1\
                    .unionByName(df2,allowMissingColumns=True), dfs_list)

        return df

    def _transform(self, df: DataFrame) -> DataFrame:
        """Creates expected columns if not exists, transforms nulls into
        "N/A" (not applicable) and removes duplicates

        Arguments:
        df (DataFrame): Raw pyspark dataframe

        Returns:
        df_processed (DataFrame): The transformed data
        """

        self.log.info("Starting data transformation...")

        missing_columns = [column for column in SchemaTables.SCHEMA \
                            if column not in df.columns]

        for column in missing_columns:
            df = df.withColumn(column, lit("N/A"))

        df_processed = df.fillna("N/A")

        df_processed = df_processed.dropDuplicates(["id"])

        return df_processed

    def _save_on_gcs(self, df: DataFrame) -> None:
        """Saves the transformed data to a GCS bucket partitioned by country,
        state and city

        Args:
            df (DataFrame): The transformed data to be saved
        """

        blob_path = self.gcs_buckets.get_path_in_layer(self.bucket_name,
                                                        self.destination_layer_name)

        self.log.info(f"Saving data to GCS: {blob_path}")
        df.write.mode("overwrite").partitionBy("country", "state").parquet(blob_path)

        self.gcs_buckets.clear_bucket(self.bucket_name, self.origin_layer_name)

    def _post_processing(self) -> None:
        """Post processing step to be executed after the transformation"""

        self.spark.stop()

        self.log.info("Transform task completed successfully")