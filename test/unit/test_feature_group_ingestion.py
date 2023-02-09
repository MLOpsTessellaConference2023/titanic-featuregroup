import unittest

import pandas

from feature_group_ingestion import cast_object_to_string

class TestCastObjectToString(unittest.TestCase):

    def setUp(self):
        self.data = pandas.DataFrame({
            'FloatCol': [1.0, 2, 3, 4, 5, 6],
            'ObjectCol': ['ABC', 'DEF', '', '', '', '']
        })

        # Get casted result
        self.result: pandas.DataFrame = cast_object_to_string(self.data)

    def test_column_types(self):
        """ Test result columns types """
        self.assertEqual(self.result.dtypes['FloatCol'], float, 'Type float col was modified')
        self.assertEqual(self.result.dtypes['ObjectCol'], 'string', 'Type object col was not casted to string')

if __name__ == '__main__':
    unittest.main()