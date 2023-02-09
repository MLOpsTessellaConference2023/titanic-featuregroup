# -*- coding: utf-8 -*-
"""
Date: 01/2023
Author: (C) Capgemini Engineering - Antonio Galan, Jose Pena

Data Wrangling
==============

Step to wrangle the data, including:
- Cleaning the data
- Format columns
- Drop irrelevant columns or rows
- Format values
...
"""
# Import local scripts in AWS
# import sys # TODO
# sys.path.insert(0, '/opt/ml/processing/code')
import numpy
from pandas import DataFrame

from utils import parse_args, data_args


@data_args
def wrangle(data: DataFrame) -> DataFrame:
    """
    Clean, select and format data

    Params:
        - data (pandas.DataFrame): The original dataset

    Return:
        - pandas.DataFrame: The processed dataset
    """
    # Columns names formatting
    data.columns = [col.lower().strip() for col in data.columns]

    # Useless columns for our purpose
    data = data.drop(
        ['cabin', 'ticket', 'name'],
        axis=1
    )

    # Normalize nans
    data = data.replace(['nan', '', ' ', None, 'NaN'], numpy.nan)

    return data


if __name__ == '__main__':
    # If you want to run it locally in your IDE, create the folder bin, and download the data in:
    # https://www.kaggle.com/datasets/heptapod/titanic/download?datasetVersionNumber=1
    # Then use the following parameters to run it:
    #   --data-path="../bin" --step-name="Data Wrangling" --output-file="wrangled_data.csv"
    #   --input-file="titanic.csv"
    args = parse_args()
    wrangle(args=args)
