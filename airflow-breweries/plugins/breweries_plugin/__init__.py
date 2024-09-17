from airflow.plugins_manager import AirflowPlugin
from breweries_plugin.gcs_buckets import GcsBuckets

class GcsPlugin(AirflowPlugin):
    name = "breweries_plugin"
    helpers = [GcsBuckets]
