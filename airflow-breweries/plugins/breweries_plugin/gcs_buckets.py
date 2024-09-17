from datetime import datetime
from google.cloud import storage
import json


class GcsBuckets:
    """ A class to interact with Google Cloud Storage buckets.
    Initializes the GcsBuckets instance with a storage client.
    """
    def __init__(self):
        self.storage_client = storage.Client()


    def get_path_in_layer(self, bucket_name: str, layer_name: str,
                            file_name: str = None) -> str:
        """ Generates a path string for a given layer and timestamp"""

        return f"gs://{bucket_name}/{layer_name}-layer/" + (file_name or "")

    def get_file_list_in_layer(self, bucket_name: str, layer_name: str) -> list:

        bucket = self.storage_client.bucket(bucket_name)

        blobs = bucket.list_blobs(prefix=f"{layer_name}-layer")

        blobs_list = [f"gs://{bucket_name}/{blob.name}" for blob in blobs]

        return blobs_list

    def save_json_to_gcs(self, data: dict, bucket_name: str, layer_name: str,
                        file_name: str,) -> None:
        """ Save a JSON object to a Google Cloud Storage bucket"""

        blob_path = f"{layer_name}-layer/{file_name}"

        bucket = self.storage_client.bucket(bucket_name)

        blob = bucket.blob(blob_path)

        blob.upload_from_string(json.dumps(data),
                            content_type="application/json")

    def clear_bucket(self, bucket_name: str, layer_name: str) -> None:
        """Clears all files in a given bucket and layer"""

        bucket = self.storage_client.bucket(bucket_name)

        blobs = bucket.list_blobs(prefix=f"{layer_name}-layer")

        for blob in blobs:
            blob.delete()