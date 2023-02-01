# -*- coding: utf-8 -*-
"""
Pipeline
========

Script to ...

Notes
-----

Date: 01/2023
Version: 1.0
Author: (C) Capgemini Engineering - Antonio Galan, Jose Pena
Website: www.capgemini.com
"""
import sys
import argparse

import multiprocessing
from pathlib import Path

import logging

import boto3
import pandas
from sagemaker.feature_store.feature_group import FeatureGroup


def get_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)  # mod INFO
    logger.addHandler(logging.StreamHandler())
    logger.propagate = False
    return logger


logger = get_logger()
SEP = ';'


def cast_object_to_string(data_frame):
    for label in data_frame.columns:
        if data_frame.dtypes[label] == "object":
            data_frame[label] = data_frame[label].astype("str").astype("string")
    return data_frame


def format_column_names(data: pandas.DataFrame):
    data.rename(columns=lambda x: x.replace(' ', '_').replace('.', '')[:62], inplace=True)
    return data


try:
    import sagemaker

except ModuleNotFoundError:
    logger.warning('Package sagemaker not found, will proceed to be instaled by subprocess lib.')
    # For installing sagemaker in the execution container if not installed
    import subprocess

    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'sagemaker'],
                          stdout=subprocess.DEVNULL,
                          stderr=subprocess.STDOUT)


def check_fg_prep(feature_group: pandas.DataFrame, record_identifier_name: str = 'TransactionID',
                  event_time_feature_name: str = 'EventTime'):
    if record_identifier_name in feature_group.columns:
        lg_fg = len(feature_group[record_identifier_name])
        lg_uniq = len(feature_group[record_identifier_name].unique())

        if lg_fg != lg_uniq:
            exc = Exception(f'\tColumn {record_identifier_name} for feature group has {abs(lg_fg - lg_uniq)}')
            logger.exception(exc)
            raise (exc)

    else:
        logger.warning(
            str(f'RecordIdentifier column with name {record_identifier_name} doesnt appear in feature group.' +
                f'\nCreating column {record_identifier_name} with values from dataframe index.'))
        feature_group[record_identifier_name] = feature_group.index

    if event_time_feature_name not in feature_group.columns:
        from datetime import datetime
        logger.warning(str(f'EventTime column with name {event_time_feature_name} doesnt appear in feature group.' +
                           f'\nCreating column {event_time_feature_name} with values from now: {datetime.now()}'))

        feature_group[event_time_feature_name] = pandas.to_datetime('now').strftime('%Y-%m-%dT%H:%M:%SZ')
        feature_group[event_time_feature_name] = feature_group[event_time_feature_name].astype(str).astype('string')

    # Short Column names and no spaces
    feature_group = format_column_names(feature_group)

    # HardCast objects to string type
    feature_group = cast_object_to_string(feature_group)

    return feature_group


def main(args):
    """
    Desc.

    Args:
        args (argparse.arguments):
            --data-path
            --featuregroup-name
    """

    # -------------- Args --------------

    parser = argparse.ArgumentParser("Gets arguments for data cleaning")

    parser.add_argument(
        "--data-path",
        dest="path",
        type=str,
        help="The path to load data. (Mandatory)"
    )

    parser.add_argument(
        "-f",
        "--featuregroup-name",
        dest="fg_name",
        type=str,
        help="The feature group name. (Mandatory)"
    )

    parser.add_argument(
        "-r",
        "--region",
        dest="region",
        type=str,
        help="The output path of the result dataframe.",
        default='eu-west-1'
    )

    args = parser.parse_args(args)

    # path and file_name are mandatory arguments
    if args.path is None:
        parser.print_help()
        sys.exit(2)

    if args.fg_name is None:
        parser.print_help()
        sys.exit(2)

    # -------------- logic --------------

    data_path = Path(args.path)

    files_to_ingest = [file for file in data_path.glob('*.csv')]

    for file in files_to_ingest:
        # load data in ram
        logger.info(f"Data reading from {file}")
        df = pandas.read_csv(filepath_or_buffer=file, sep=SEP, header=0, low_memory=False)

        boto_session = boto3.session.Session(region_name=args.region)
        sagemaker_session = sagemaker.Session(boto_session=boto_session)

        n_workers = round(len(df) / 5e4)
        logger.info(f"Number of workers: {n_workers} for a total number of rows of {len(df)}")
        n_processes = multiprocessing.cpu_count()
        logger.info(f"Number of processes: {n_processes} for a CPU count of {n_processes}")

        logger.info(f'Ingesting rows into Feature Group: {args.fg_name}')
        fg = FeatureGroup(name=args.fg_name, sagemaker_session=sagemaker_session)

        fg_description = fg.describe()

        df = check_fg_prep(feature_group=df, record_identifier_name=fg_description['RecordIdentifierFeatureName'],
                           event_time_feature_name=fg_description['EventTimeFeatureName'])
        fg.ingest(data_frame=df, max_workers=n_workers, max_processes=n_processes, wait=True)


if __name__ == "__main__":
    main(sys.argv[1:])
