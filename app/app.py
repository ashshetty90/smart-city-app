from app.process_taxi_trip_data import ProcessTaxiTripData
from app.process_taxi_ev_list import ProcessTaxiEvData
from app.process_census_tracks import ProcessCensusTracks
from app.process_gps_data import ProcessGpsData
from helpers.dask_helper import DaskHelper
from helpers.sql_helper import SqlHelper
import logging

# initialise loggers
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

OUTPUT_CSV_DIR = {
    "taxi_trip_df": "data/taxi_trip_clean",
    "ev_df": "data/ev_clean",
    "e_vehicle_gps_df": "data/e_vehicle_gps_clean",
    "poly_df": "data/poly_clean"
}


class SmartCityApp:
    def __init__(self):
        logger.info("Instantiating source data classes ")
        self.ev_data = ProcessTaxiEvData()
        self.census_tracts = ProcessCensusTracks()
        self.gps_data = ProcessGpsData()
        self.taxi_trips = ProcessTaxiTripData()
        self.dask_helper = DaskHelper()
        self.sql_helper = SqlHelper()

    def run(self):
        """
       Fetching data from various data sources and aggregating the output
       """
        try:
            logger.info("Fetching Taxi EV data")
            ev_df = self.ev_data.get()
            logger.info("Writing Taxi EV data to CSV")
            self.dask_helper.to_csv(ev_df, OUTPUT_CSV_DIR.get("ev_df"))
            logger.info("Fetching Polygon data")
            poly_df = self.census_tracts.get()
            logger.info("Writing Polygon data to CSV")
            self.dask_helper.to_csv(poly_df, OUTPUT_CSV_DIR.get("poly_df"))
            logger.info("Fetching Taxi GPS data")
            gps_df = self.gps_data.get()
            logger.info("Fetching Taxi Trip data")
            taxi_trip_df = self.taxi_trips.get()
            self.dask_helper.to_csv(taxi_trip_df, OUTPUT_CSV_DIR.get("taxi_trip_df"))

            # Merging the gps data frame with the ev data frame
            # to get all electric taxis with their respective gps data

            e_vehicle_gps_df = gps_df.merge(ev_df, how="inner", on=["company", "taxi_id"])

            # Merging e_vehicle_gps_df dataframe with data returned
            # from taxi trip data in BQ to get all currently active taxis

            e_vehicle_gps_df = e_vehicle_gps_df.merge(taxi_trip_df, how="inner", on=["company", "taxi_id"])
            logger.info("Writing e-vehicle gps data to csv")
            self.dask_helper.to_csv(e_vehicle_gps_df, OUTPUT_CSV_DIR.get("e_vehicle_gps_df"))

        except Exception as e:
            logger.error("Error while processing data", e)


