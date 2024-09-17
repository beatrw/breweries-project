import os
import sys
import pytest
import requests

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))

plugin_path = os.path.join(project_root, 'plugins')
if plugin_path not in sys.path:
    sys.path.append(plugin_path)

from breweries_plugin.gcs_buckets import GcsBuckets
from dags.breweries.tasks.landing_task import DataExtractor

@pytest.fixture(scope="module")
def extractor():
    return DataExtractor()

def test_extract_data_from_api(extractor):
    """Test if the API data extraction returns a list with data"""

    data = extractor._extract()

    assert isinstance(data, list)

    if data:
        assert isinstance(data[0], dict)

def test_extract_multiple_pages(extractor):
    """Test if extracting multiple pages returns a list"""

    data = extractor._extract()

    assert len(data) > 200, "Expected more than 200 records, indicating multiple pages"

def test_api_status_code(extractor):
    """Test if the API responds with status 200"""

    url = "https://api.openbrewerydb.org/v1/breweries"

    response = requests.get(url, params={"page": 1, "per_page": 1})

    assert response.status_code == 200, f"API request failed with status {response.status_code}"