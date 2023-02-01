# -*- coding: utf-8 -*-
"""
Date: 01/2023
Author: (C) Capgemini Engineering - Antonio Galan, Jose Pena

Feature Validation
==================

Step to validate the values of the features to check data consistency, and apply business rules or constraints
"""
import pandas
import pandera

from pandera import Column, Check, Hypothesis
from utils import data_args, parse_args


@data_args
def validate_features(data: pandas.DataFrame) -> pandas.DataFrame:
    """"
    Method to validate the features
    """
    # PassengerId,target,pclass,sex,age,sibsp,parch,fare,embarked,hours_traveling,alone
    schema = pandera.DataFrameSchema(
        {
            "target": Column(
                "int64",
                [
                    Check.isin([0,1]),
                    # Hypothesis testing: the number of women that survived
                    # was greater than the one of men
                    Hypothesis.two_sample_ttest(
                        sample1="male",
                        sample2="female",
                        groupby="sex",
                        alpha=0.05,
                        equal_var=True,
                        relationship="greater_than",
                    ),
                ],
            ),
            "sex": Column("str", [Check.isin(["male", "female"])]),
        }
    )
    return data


if __name__ == '__main__':
    # If you want to run it locally in your IDE, create the folder bin and use the following parameters:
    #   --data-path="../bin" --step-name="Features Processing" --input-file="processed_features.csv"
    args = parse_args()
    validate_features(args=args)
