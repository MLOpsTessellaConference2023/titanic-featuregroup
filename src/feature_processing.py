# -*- coding: utf-8 -*-
"""
Date: 01/2023
Author: (C) Capgemini Engineering - Antonio Galan, Jose Pena

Feature Processing
==================

Step to process the features, including tasks like:
- Creating, modifying and combining features
- Adjust features
- Deal with dates
...
"""
import numpy
import pandas

from utils import data_args, parse_args


@data_args
def process_features(data: pandas.DataFrame):
    """
    Create, modify or combine features

    Params:
        data (pandas.DataFrame): The original dataset

    Return:
        pandas.DataFrame: The processed dataset
    """

    # Create new features
    data['hours_traveling'] = data['embarked'].replace({
        'Q': 85,
        'C': 102,
        'S': 108
    }).astype(float)

    # Transform features
    data['age_group'] = pandas.cut(
        data['age'],
        [0, 3, 10, 18, 30, 50, 70, 100],
        labels=['babies', 'children', 'teenagers', 'young', 'adults', 'seniors', 'elders']
    )
    data = data.drop(['age'], axis=1)

    # Combine features
    data['alone'] = numpy.where((data['sibsp'] == 0) & (data['parch'] == 0), 'yes', 'no')

    # Fix features
    data['fare'] = data['fare'] * 100  # Inflation correction (GBP UKCPI2005)

    return data


if __name__ == '__main__':
    # If you want to run it locally in your IDE, create the folder bin and use the following parameters:
    #   --data-path="../bin" --step-name="Features Processing" --output-file="processed_features.csv"
    #   --input-file="wrangled_data.csv"
    args = parse_args()
    process_features(args=args)
