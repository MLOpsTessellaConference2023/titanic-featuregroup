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
from time import perf_counter

import pandas


def parse_args() -> argparse.Namespace:
    """
    Method to parse the arguments need to execute the pipeline step

    Returns:
        argparse.Namespace: object with the arguments used for the pipeline step.
            --step-name
            --data-path
            --input-file
            --output-file
    """
    parser = argparse.ArgumentParser()

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

    args, _ = parser.parse_known_args()

    print(f"Received arguments:\n{args}.\n")

    return args


def data_args(data_step):
    """Decorator with the parser arguments need to the feature processing pipeline"""

    def execute(args: argparse.Namespace) -> pandas.DataFrame:
        print(f"Starting step {args.step_name}")
        starting_time = perf_counter()

        # CSV file read
        data = pandas.read_csv(filepath_or_buffer=f"{args.data_path}/{args.input_file}",
                               index_col='PassengerId',
                               low_memory=False,
                               sep=','
                               )

        data = data_step(data=data)

        if args.output_file is not None:
            data.to_csv(path_or_buf=f"{args.data_path}/{args.output_file}",
                        sep=',',
                        index=True)

        total_time = perf_counter() - starting_time
        print(f"Finished {args.step_name} Step after {total_time:.4f} seconds.\n\n")
        return data_step

    return execute
