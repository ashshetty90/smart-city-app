import logging
import dask.dataframe

# initialize logger
logger = logging.getLogger('dask_helper')
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)




class DaskHelper:
    """
    Helper class to read and write data as a dask dataframe
    """

    def __init__(self, file_path=None, dtype=None):
        self.file_path = file_path
        self.data_types = dtype

    def read_csv(self):
        """
       read csv file from local filesystem and convert to a dask dataframe
       :return: a dask dataframe of the csv
       """
        logger.info("Reading csv file from {0}".format(self.file_path))
        return dask.dataframe.read_csv(self.file_path, dtype=self.data_types)

    def to_csv(self,output_df, output_location):
        """
       outputs the csv format of the dataframe to a local directory
       :param output_df: dataframe to be converted to csv
       :param output_location: location on the local filesystem to drop the csv
       """
        logger.info("Writing csv file to {0}".format(output_location))
        output_df.drop_duplicates(ignore_index=True).to_csv(output_location, index=False)

