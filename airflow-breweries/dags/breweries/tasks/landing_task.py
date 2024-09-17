import requests
from datetime import datetime

from breweries_plugin.gcs_buckets import GcsBuckets
from breweries_plugin.logging_config import configure_logging


class DataExtractor:
    """Class responsible for extracting data
    from openbrewerydb API and saving it to GCS Buckets
    """

    def __init__(self):

        self.log = configure_logging()

    def execute(self) -> None:
        """Execute the flow order of functions"""

        self.bucket_name = "storage-breweries"
        self.destination_layer_name = "bronze"

        data = self._extract()

        self._save_on_gcs(data)


    def _extract(self) -> list:
        """Extracts data from openbrewerydb API

        Returns:
            data (list): The extracted data
        """

        self.log.info("Starting data extraction...")

        url = "https://api.openbrewerydb.org/v1/breweries"
        session = requests.Session()
        per_page = 200
        page = 1
        all_data = []

        while True:
            response = session.get(url, params={"page": page, "per_page": per_page})
            response.raise_for_status()

            data = response.json()

            if not data:
                break

            all_data.extend(data)

            page += 1

        return all_data

    def _save_on_gcs(self, data: list) -> None:
        """Saves the extracted data to a GCS bucket

        Args:
            data (list): The extracted data
        """

        timestamp = datetime.now()
        file_name = f"{timestamp.strftime('%Y%m%d')}.json"

        gcs_buckets = GcsBuckets()
        gcs_buckets.save_json_to_gcs(data, self.bucket_name,
                                    self.destination_layer_name, file_name)

        self.log.info("Data extraction completed successfully")