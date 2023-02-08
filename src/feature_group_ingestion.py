# -*- coding: utf-8 -*-
"""
Date: 01/2023
Version: 1.0
Author: (C) Capgemini Engineering - Antonio Galan, Jose Pena
Website: www.capgemini.com


Feature Group Ingestion
=======================

Script to ingest a pandas.DataFrame into a Feature Group

"""
import sys
import logging
import multiprocessing

import boto3
import pandas
from sagemaker.feature_store.feature_group import FeatureGroup

from utils import data_args, parse_args


def get_logger():
    _logger = logging.getLogger(__name__)
    _logger.setLevel(logging.INFO)  # mod INFO test
    _logger.addHandler(logging.StreamHandler())
    _logger.propagate = False
    return _logger


logger = get_logger()
SEP = ';'


def cast_object_to_string(data_frame: pandas.DataFrame):
    obj_cols = data_frame.select_dtypes(["object"]).columns.tolist()
    data_frame[obj_cols] = data_frame[obj_cols].astype("str").astype("string")
    return data_frame


try:
    import sagemaker

except ModuleNotFoundError:
    logger.warning('Package sagemaker not found, will proceed to be instaled by subprocess lib.')
    # For installing sagemaker in the execution container if not installed
    import subprocess

    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'sagemaker'],
                          stdout=subprocess.DEVNULL,
                          stderr=subprocess.STDOUT)


def check_fg_prep(features: pandas.DataFrame,
                  record_identifier_name: str = 'TransactionID',
                  event_time_feature_name: str = 'EventTime'):

    if record_identifier_name in features.columns:
        lg_fg = len(features[record_identifier_name])
        lg_uniq = len(features[record_identifier_name].unique())

        if lg_fg != lg_uniq:
            exc = Exception(f'\tColumn {record_identifier_name} for feature group has {abs(lg_fg - lg_uniq)}')
            logger.exception(exc)
            raise (exc)

    else:
        logger.warning(
            str(f'RecordIdentifier column with name {record_identifier_name} doesnt appear in feature group.' +
                f'\nCreating column {record_identifier_name} with values from dataframe index.'))
        features[record_identifier_name] = features.index

    if event_time_feature_name not in features.columns:
        from datetime import datetime
        logger.warning(str(f'EventTime column with name {event_time_feature_name} doesnt appear in feature group.' +
                           f'\nCreating column {event_time_feature_name} with values from now: {datetime.now()}'))

        features[event_time_feature_name] = pandas.to_datetime('now').strftime('%Y-%m-%dT%H:%M:%SZ')
        features[event_time_feature_name] = features[event_time_feature_name].astype(str).astype('string')

    # HardCast objects to string type
    features = cast_object_to_string(features)

    return features


@data_args
def ingest(data: pandas.DataFrame) -> None:
    """
    Desc.

    Args:
        data (pandas.DataFrame):
    """
    boto_session = boto3.session.Session(region_name=args.region)
    sagemaker_session = sagemaker.Session(boto_session=boto_session)

    n_workers = round(len(data) / 5e4)
    print(f"Number of workers: {n_workers} for a total number of rows of {len(data)}")

    n_processes = multiprocessing.cpu_count()
    print(f"Number of processes: {n_processes} for a CPU count of {n_processes}")

    print(f'Ingesting rows into Feature Group: {args.fg_name}')
    feature_group = FeatureGroup(name=args.fg_name,
                                 sagemaker_session=sagemaker_session)

    fg_description = feature_group.describe()

    data = check_fg_prep(features=data,
                         record_identifier_name=fg_description['RecordIdentifierFeatureName'],
                         event_time_feature_name=fg_description['EventTimeFeatureName'])

    feature_group.ingest(data_frame=data, max_workers=n_workers, max_processes=n_processes, wait=True)


if __name__ == "__main__":

    # -------------- Args --------------

    parser = parse_args(message="Gets arguments for data cleaning",
                        return_parser=True)

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

    args, _ = parser.parse_known_args()

    # path and file_name are mandatory arguments
    if args.path is None or args.fg_name is None or \
            args.input_file is None:
        parser.print_help()
        sys.exit(2)

    print(f"Received arguments:\n{args}.\n")

    ingest(args=args)
