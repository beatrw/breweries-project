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
from dags.breweries.tasks.transform_task import DataTrasformer

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("pytest").getOrCreate()

@pytest.fixture(scope="module")
def transformer():
    return DataTrasformer()

def test_transform_with_missing_columns(spark, transformer):
    """Test transformation with missing columns"""

    data = [Row(id=1, name="Brewery 1"), Row(id=2, name="Brewery 2")]
    df = spark.createDataFrame(data)

    df_transformed = transformer._transform(df)

    assert "state" in df_transformed.columns
    assert "country" in df_transformed.columns

    result = df_transformed.filter(df_transformed["state"] == "N/A").count()
    assert result == 2

def test_transform_with_duplicates(spark, transformer):
    """Test transformation removing duplicates"""

    data = [Row(id=1, name="Brewery 1"), Row(id=1, name="Brewery 1 - duplicate")]
    df = spark.createDataFrame(data)

    df_transformed = transformer._transform(df)

    assert df_transformed.count() == 1
    assert df_transformed.filter(df_transformed["id"] == 1).count() == 1

def test_transform_without_modifications(spark, transformer):
    """Test transformation when all columns already exist and no duplicates"""

    data = [
        Row(id=1, name="Brewery 1", state="RN", country="BRAZIL"),
        Row(id=2, name="Brewery 2", state="RJ", country="BRAZIL")
    ]
    df = spark.createDataFrame(data)

    df_transformed = transformer._transform(df)

    assert df_transformed.count() == 2

    assert "state" in df_transformed.columns
    assert "country" in df_transformed.columns
    assert df_transformed.filter(df_transformed["state"] == "N/A").count() == 0
    assert df_transformed.filter(df_transformed["country"] == "N/A").count() == 0
