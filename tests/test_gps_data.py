import unittest

import dask

from app.process_gps_data import ProcessGpsData


class TestGpsData(unittest.TestCase):
    def setUp(self) -> None:
        self.process_gps = ProcessGpsData()

    def test_get(self):
        gps_df = self.process_gps.get()
        self.assertIs(type(gps_df), dask.dataframe.core.DataFrame)


if __name__ == '__main__':
    unittest.main()
