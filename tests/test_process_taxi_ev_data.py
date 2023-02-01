import unittest

import dask

from app.process_taxi_ev_list import ProcessTaxiEvData


class TestTaxiEv(unittest.TestCase):
    def setUp(self) -> None:
        self.taxi_ev = ProcessTaxiEvData()

    def test_get(self):
        ev_df = self.taxi_ev.get()
        self.assertIs(type(ev_df), dask.dataframe.core.DataFrame)


if __name__ == '__main__':
    unittest.main()
