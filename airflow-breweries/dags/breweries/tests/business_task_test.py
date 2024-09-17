import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))

plugin_path = os.path.join(project_root, 'plugins')
if plugin_path not in sys.path:
    sys.path.append(plugin_path)

from breweries_plugin.gcs_buckets import GcsBuckets
from breweries_plugin.spark_config import create_spark_session
from dags.breweries.tasks.business_task import DataRulesInjector


@pytest.fixture(scope="module")
def injector():
    return DataRulesInjector()


def test_aggregate_data(injector):
    """Tests the aggregation of data by country, state, and brewery type"""

    spark = create_spark_session('business_test')
    data = [
        Row(country="Brazil", state="Acre", brewery_type="micro"),
        Row(country="Brazil", state="Acre", brewery_type="micro"),
        Row(country="Brazil", state="Pernambuco", brewery_type="brewpub"),
        Row(country="Canada", state="Ontario", brewery_type="regional"),
    ]
    df = spark.createDataFrame(data)

    df_aggregated = injector._agregate_data(df)

    result = df_aggregated.collect()

    assert len(result) == 4, "The number of rows is incorrect"

    expected_counts = {
        ("Brazil", "Acre", "micro"): 2,
        ("Brazil", "Pernambuco", "brewpub"): 1,
        ("Canada", "Ontario", "regional"): 1,
    }

    for row in result:
        key = (row["country"], row["state"], row["brewery_type"])
        assert row["brewery_count"] == expected_counts[key], f"Contagem incorreta para {key}"

