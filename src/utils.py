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
import argparse
import json

from datetime import datetime
from time import perf_counter

import pandas


def parse_args(message: str="", return_parser: bool=False) -> argparse.Namespace | argparse.ArgumentParser:
    """
    Method to parse the arguments need to execute the pipeline step

    Returns:
        argparse.Namespace: object with the arguments used for the pipeline step.
            --step-name
            --data-path
            --input-file
            --output-file
    """
    parser = argparse.ArgumentParser(message)

    parser.add_argument("--step-name",
                        dest="step_name",
                        type=str,
                        default="generic",
                        help="Name of the step to be executed")

    parser.add_argument("--data-path",
                        dest="data_path",
                        type=str,
                        help="Path to work with data")

    parser.add_argument("--input-file",
                        dest="input_file",
                        type=str,
                        help="Filename to extract the data from")

    parser.add_argument("--output-file",
                        dest="output_file",
                        type=str,
                        help="Filename to store the output data")

    if return_parser:
        return parser

    args, _ = parser.parse_known_args()

    print(f"Received arguments:\n{args}.\n")

    return args


def data_args(data_step):
    """Decorator with the parser arguments need to the feature processing pipeline"""

    def execute(args: argparse.Namespace) -> pandas.DataFrame | None:
        print(f"Starting step {args.step_name}")
        starting_time = perf_counter()

        # CSV file read
        data = pandas.read_csv(filepath_or_buffer=f"{args.data_path}/{args.input_file}",
                               index_col='PassengerId',
                               low_memory=False,
                               sep=','
                               )

        if data.empty:
            raise Exception('Input Dataframe is empty.')

        data = data_step(data=data)
        total_time = perf_counter() - starting_time

        if args.output_file is not None:
            if data.empty:
                raise Exception('Dataframe as result is empty.')

            data.to_csv(path_or_buf=f"{args.data_path}/{args.output_file}",
                        sep=',',
                        index=True)

        if isinstance(data, pandas.DataFrame):
            dw_summary = {'date': datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S"),
                          'n_samples': data.shape[0],
                          'n_features': data.shape[1],
                          '%nan_values': round((data.isna().sum().sum() / (data.shape[0] * data.shape[1])) * 100, 2),
                          'prop_target': dict(
                              round(data['survived'].value_counts(normalize=True, dropna=False), 3)),
                          'time': total_time,
                          'features': data.columns.tolist()
                          }

            print(f'{"_" * 20} Summary {"_" * 20}\n\n{json.dumps(dw_summary, indent=4)}')

        print(f"Finished {args.step_name} Step after {total_time:.4f} seconds.\n\n")

        return data_step

    return execute
