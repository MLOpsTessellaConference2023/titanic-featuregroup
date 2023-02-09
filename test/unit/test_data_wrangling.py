import unittest

import pandas
import numpy

from undecorated import undecorated

from data_wrangling import wrangle

class TestWrangle(unittest.TestCase):

    def setUp(self):
        self.data = pandas.DataFrame({
            'Cabin': ['C123', 'C456', '', '', '', ''],
            'Ticket': ['ABC', 'DEF', '', '', '', ''],
            'Name': ['John', 'Alice', 'Bob', '', '', ''], 
            ' AgE ': [20, 'nan', '', ' ', None, 'NaN']
        })

        # Get undecorated wrangle result
        wrangle_undecorated: function = undecorated(wrangle)
        self.result: pandas.DataFrame = wrangle_undecorated(self.data)

    def test_return_type(self):
        """ Test proper result type """
        self.assertEqual(type(self.result), pandas.DataFrame, 'Return type is not pandas.DataFrame')
    
    def test_columns_format(self):
        """ Test columns format """
        self.assertIn('age', self.result.columns, 'Columns not transformed to lowerCase and strip')

    def test_empty_values(self):
        """ Test empty values are numpy.nan """
        self.assertEqual(self.result['age'].value_counts(dropna=False)[numpy.nan], 5, 'Not all empty values were caught')

if __name__ == '__main__':
    unittest.main()