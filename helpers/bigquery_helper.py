from google.cloud import bigquery
from google.cloud.storage import Client as StorageClient
import logging
# initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class BigQueryHelper:
    """
    Helper class to connect to BigQuery via API
    See https://googlecloudplatform.github.io/google-cloud-python/latest/bigquery/usage.html
    For authentication set the env variable GOOGLE_APPLICATION_CREDENTIALS with the path to the json file.
    """
    TIMEOUT = 30  # in seconds

    def __init__(self, project_id):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    def get_datasets(self):
        return self.client.list_datasets()

    def get_dataset(self):
        return self.client.get_dataset(self.dataset_ref)

    def get_tables(self, dataset):
        return self.client.list_dataset_tables(dataset)

    def get_table_ref(self,table_id):
        return self.dataset_ref.table(table_id)

    def get_table(self,table_ref):
        return self.dataset_ref.table(table_ref)

    def query(self, query):
        query_job = self.client.query(query)  # API request - starts the query
        # Waits for the query to finish
        iterator = query_job.result(timeout=self.TIMEOUT)
        # rows = list(iterator)
        return iterator

    def extract(self, bucket_name, table_ref, destination_blob_name):
        storage_client = StorageClient()
        bucket = storage_client.create_bucket(bucket_name)  # API request
        destination = bucket.blob(destination_blob_name)

        destination_uri = 'gs://{}/{}'.format(bucket_name, destination_blob_name)
        extract_job = self.client.extract_table(
            table_ref, destination_uri)  # API request
        extract_job.result(timeout=100)  # Waits for job to complete.

        got = destination.download_as_string().decode('utf-8')  # API request

    def load(self):
        pass
