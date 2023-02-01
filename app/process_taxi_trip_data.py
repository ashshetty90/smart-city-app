import logging
from helpers.bigquery_helper import BigQueryHelper
from google.cloud.storage import Client as StorageClient
PROJECT_ID = "bigquery-public-data"
DATASET_ID = "chicago_taxi_trips"
PROJECT_ID = "gsheet-poc-1565062565011"


# initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class ProcessTaxiTripData:
    def __init__(self):
        """
        Initialising bigquery helper to return a client to connect based on the project id
        """
        self.bq_client = BigQueryHelper(project_id=PROJECT_ID)

    def get(self):
        """
        fetch the last trip information of a taxi from bigquery. Columns
        fetched are taxi_id, company and the last trip timestamp
        :returns a dask dataframe
        """
        try:
            logger.info("Fetching taxi trip data from big query")
            first_query = """SELECT taxi_id,company,trip_end_timestamp FROM (
                        SELECT taxi_id,company,trip_end_timestamp,ROW_NUMBER() OVER (
                        PARTITION BY company,taxi_id ORDER BY trip_end_timestamp desc) as row_num 
                        FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`)
                        de_dup 
                        WHERE row_num=1
                """
            bq_storage_client = StorageClient()
            logger.info("Fetching completed successfully")
            return self.bq_client.query(first_query).\
                to_dataframe(bqstorage_client=bq_storage_client).drop_duplicates(ignore_index=True)

        except Exception as e:
            logger.error("Error while fetching data in class {0}".format(__name__), e)
