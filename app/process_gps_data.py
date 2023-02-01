import logging
import os

from helpers.dask_helper import DaskHelper
from app import FILE_DIR

GPS_FILE_PATH = os.path.join(FILE_DIR, 'gps_data', '*.csv')

# initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class ProcessGpsData:
    def __init__(self):
        self.dask_helper = DaskHelper(file_path=GPS_FILE_PATH)

    def get(self):
        logger.info("Processing GPS Data")
        try:
            return self.dask_helper.read_csv().drop_duplicates(ignore_index=True)
        except Exception as e:
            logger.error("Error while fetching data in class {0}".format(__name__), e)


