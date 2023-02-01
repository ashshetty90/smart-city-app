import unittest

from helpers.bigquery_helper import BigQueryHelper

from google.cloud.bigquery.table import RowIterator


class TestBqHelper(unittest.TestCase):
    def setUp(self) -> None:
        self.bq_client = BigQueryHelper(project_id="gsheet-poc-1565062565011")

    def test_query(self):
        test_query = """SELECT taxi_id,company,trip_end_timestamp 
                    FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
                    limit 10
            """
        self.assertIsInstance(self.bq_client.query(test_query), RowIterator)


if __name__ == '__main__':
    unittest.main()
