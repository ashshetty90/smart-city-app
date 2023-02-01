import logging
from app import FILE_DIR
import os
from helpers.dask_helper import DaskHelper

EV_FILE_PATH = os.path.join(FILE_DIR, "taxi_ev_list.csv")
EV_OUTPUT_PATH = ""
# initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class ProcessTaxiEvData:
    def __init__(self):
        self.dask_helper = DaskHelper(file_path=EV_FILE_PATH)

    def get(self):
        """
        fetch all the electric cars from ev list csv file
       :returns a dask dataframe
       """
        try:
            df = self.dask_helper.read_csv()
            return df[df.is_electric == 1].drop_duplicates(ignore_index=True)

        except Exception as e:
            logger.error("Error while fetching data in class {0}".format(__name__), e)
