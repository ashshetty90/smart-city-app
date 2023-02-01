import logging
import os
from app import FILE_DIR
from helpers.dask_helper import DaskHelper

CENSUS_TRACK_FILE_PATH = os.path.join(FILE_DIR, "census_tracts.csv")
# initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

DATA_TYPES = {'the_geom': 'object', 'STATEFP': 'int64', 'COUNTYFP': 'int64',
              'TRACTCE': 'int64', 'GEOID': 'int64', 'NAME': 'float64',
              'NAMELSAD': 'object', 'COMMAREA': 'int64', 'COMMAREA_N': 'int64',
              'NOTES': 'object', 'LOW_EMISSION_ZONE': 'bool', 'CENSUS_YEAR': 'object'}


class ProcessCensusTracks:
    def __init__(self):
        """
        Initialising dask helper with the csv file location and explicit data type if needed
        """
        self.dask_helper = DaskHelper(file_path=CENSUS_TRACK_FILE_PATH, dtype=DATA_TYPES)

    def get(self):
        """
        fetch all the polygons which fall under the low emission zone
        :returns a dask dataframe
        """
        logger.info("Processing census data")
        try:
            df = self.dask_helper.read_csv()
            return df['the_geom'][df['LOW_EMISSION_ZONE'] == 1].to_frame().drop_duplicates(ignore_index=True)
        except Exception as e:
            logger.error("Error while fetching data in class {0}".format(__name__), e)
