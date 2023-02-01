import unittest
from helpers.dask_helper import DaskHelper
import dask


class TestDaskHelper(unittest.TestCase):
    def setUp(self) -> None:
        self.dask_helper = DaskHelper(file_path="data/test_data.csv")

    def test_read_csv(self):
        test_df = self.dask_helper.read_csv()
        self.assertIs(type(test_df), dask.dataframe.core.DataFrame)


if __name__ == '__main__':
    unittest.main()
