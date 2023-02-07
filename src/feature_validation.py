# -*- coding: utf-8 -*-
"""
Date: 01/2023
Author: (C) Capgemini Engineering - Antonio Galan, Jose Pena

Feature Validation
==================

Step to validate the values of the features to check data consistency, enforce schema of data, apply
 business rules or constraints ...
"""
import pandas
import pandera

from pandera import Column, Check
from utils import data_args, parse_args


@data_args
def validate_features(data: pandas.DataFrame) -> pandas.DataFrame:
    """"
    Method to validate the features
    """

    schema = pandera.DataFrameSchema(
        {
            # Values defined in the list must appear in the column `survived`
            "survived": Column("int64", [
                Check.unique_values_eq([0, 1])
            ]),
            # Values defined in the list must appear in the column `sex`
            "sex": Column("str", [
                Check.unique_values_eq(["male", "female"])
            ]),
            # Values must be 1, 2 or 3 and column must be of int type
            "pclass": Column("int64", [
                Check.in_range(0,3)
            ]),
            # age_group column could have the values in the list
            "age_group": Column(dtype="str",
                                checks=[
                                    Check.isin(['young', 'adults', 'teenagers', 'seniors', 'children',
                                                'babies', 'elders'
                                                ])],
                                nullable=True
            ),
            # sibsp column could have the values in range(0,8) but not equals to 6 or 7
            "sibsp": Column("int64", [
                Check.notin([6, 7]),
                Check.in_range(0, 8)
            ]),
            # alone column could have the values in the list
            "alone": Column("str", [
                Check.isin(["yes", "no"])
            ]),
            # parch column must be int within the range(0,6)
            "parch": Column("int64", [
                Check.in_range(0, 6)
            ]),
            # fare must be in range(0.0, 60000.0)
            "fare": Column("float64", [
                Check.less_than(60000.0),
                Check.greater_than_or_equal_to(0.0)
            ]),
            # embarked
            "embarked": Column(dtype="str",
                               checks=[Check.isin(['S', 'C', 'Q'])],
                               nullable=True
            ),
            #
            "hours_traveling": Column(dtype="float64",
                                      checks=[Check.in_range(80.0, 110.0)],
                                      nullable=True
            )

        },
        checks=[Check(
            lambda dataframe: all('.' not in c and ' ' not in c and len(c) <= 62 for c in dataframe.columns),
            name='Feature Group requirements',
            error='Column name does not fit feature group requirements'
        )]
    )

    # Executing feature validation
    schema(dataframe=data)
    return data


if __name__ == '__main__':
    # If you want to run it locally in your IDE, create the folder bin and use the following parameters:
    #   --data-path="../bin" --step-name="Features Validation" --input-file="processed_features.csv"
    args = parse_args()
    validate_features(args=args)
