import unittest

from app.app import SmartCityApp


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.app = SmartCityApp()

    def test_something(self):
        self.assertEquals(hasattr(self.app, "ev_data"), True)
        self.assertEquals(hasattr(self.app, "census_tracts"), True)
        self.assertEquals(hasattr(self.app, "gps_data"), True)
        self.assertEquals(hasattr(self.app, "taxi_trips"), True)
        self.assertEquals(hasattr(self.app, "dask_helper"), True)
        self.assertEquals(hasattr(self.app, "sql_helper"), True)


if __name__ == '__main__':
    unittest.main()
