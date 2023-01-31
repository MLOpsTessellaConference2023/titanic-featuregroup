# -*- coding: utf-8 -*-
"""
Pipeline
========

Script to ...

Notes
-----

Date: 01/2023
Author: (C) Capgemini Engineering - Antonio Galan, Jose Pena
"""
import numpy
import pandas

def process_features(data: pandas.DataFrame):
    '''
    Create, modify or combine features

    Params:
        - raw_data (pandas.DataFrame): The original dataset

    Return:
        - pandas.DataFrame: The processed dataset
    '''

    # Create new features
    data['hours_traveling'] = data['embarked'].replace({
        'Q': 85,
        'C': 102,
        'S': 108
    }).astype(float)

    # Transform features
    data['age'] = pandas.cut(
        data['age'],
        [0, 3, 10, 18, 30, 50, 70, 100], 
        labels=['babies', 'children', 'teenagers', 'young', 'adults', 'seniors', 'elders']
    )

    # Combine features
    data['alone'] = numpy.where((data['sibsp'] == 0) & (data['parch'] == 0), 'yes', 'no')

    # Fix features
    data['fare'] = data['fare'] * 100 # Inflation correction (GBP UKCPI2005)

    return data
