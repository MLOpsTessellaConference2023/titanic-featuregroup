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
import pandas
from argparse import ArgumentParser


def wrangle(raw_data: pandas.DataFrame):
    '''
    Clean and normalize data

    Params:
        - raw_data (pandas.DataFrame): The original dataset

    Return:
        - pandas.DataFrame: The processed dataset
    '''

    data = raw_data.copy()

    # Columns names normalization
    data.columns = data.columns.str.lower()

    # Useless columns for our purpose
    data = data.drop(
        ['cabin', 'ticket', 'name'],
    axis=1)

    # Identify target column
    data = data.rename(columns={'survived': 'target'})

    return data


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--train-test-split-ratio", type=float, default=0.3)
    args, _ = parser.parse_known_args()

    print("Received arguments {}".format(args))
