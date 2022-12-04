import unittest
from stock_exchanges import HKEX, HSI, KRX
import pandas as pd


class Test(unittest.TestCase):
    def test_output_type(self):
        """
        Test that it return dataframe
        """
        result = HKEX.aggregated_short_directory()
        self.assertIsInstance(result, pd.DataFrame)


if __name__ == "__main__":
    unittest.main()
