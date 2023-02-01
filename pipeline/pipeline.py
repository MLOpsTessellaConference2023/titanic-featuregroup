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
import sagemaker
import boto3

from sagemaker.workflow.pipeline_context import LocalPipelineSession
from sagemaker import get_execution_role

from sagemaker.workflow.steps import Step, StepTypeEnum, ProcessingStep
from sagemaker.workflow.pipeline import Pipeline

from sagemaker.processing import FrameworkProcessor

from sagemaker.sklearn.estimator import SKLearn

REGION = 'eu-west-1'
ROLE = 'local'
FRAMEWORK_VERSION = "0.20.0"

boto_session = boto3.session.Session(region_name=REGION,
                                     aws_access_key_id="",
                                     aws_secret_access_key="",
                                     aws_session_token="",
                                     )

pipeline_session = LocalPipelineSession(boto_session=boto_session)
# ROLE = get_execution_role(sagemaker_session=pipeline_session)

config = pipeline_session.config if pipeline_session.config else {}
config['region'] = 'eu-west-1'
config['local_code'] = True

pipeline_session.config = config

script_processor = FrameworkProcessor(
    sagemaker_session=pipeline_session,
    role=ROLE,
    instance_count=1,
    instance_type="local",
    estimator_cls=SKLearn,
    framework_version=FRAMEWORK_VERSION
)

from sagemaker.processing import ProcessingInput, ProcessingOutput

base_path = 'titanic-featuregroup'

input_data = r'file:///bin/titanic.csv'

inputs = [ProcessingInput(source=input_data, destination="/opt/ml/processing/input")]
outputs = [
    ProcessingOutput(output_name="train_data", source="/opt/ml/processing/train"),
    ProcessingOutput(output_name="test_data", source="/opt/ml/processing/test"),
]

steps = [
    ProcessingStep(
        name="WrangleData",
        step_args=script_processor.run(
            code="data_wrangling.py",
            source_dir='src',   # if there is a requirements.txt in src it will install those dependencies
            inputs=inputs,
            outputs=outputs,
            arguments=["--data-path", input_data]
        )
    ),
    # Step(
    #     name="FeatureEngineering",
    #     step_type=StepTypeEnum.PROCESSING,
    # ),
    # Step(
    #     name="FeatureValidation",
    #     step_type=StepTypeEnum.PROCESSING,
    # ),
    # Step(
    #     name="IngestFeatureGroup",
    #     step_type=StepTypeEnum.PROCESSING,
    # ),
]

if __name__ == '__main__':
    pipeline = Pipeline(
        name="titanic-featuregroup",
        steps=steps,
        sagemaker_session=pipeline_session
    )

    pipeline.create(
        role_arn=ROLE,  # sagemaker.get_execution_role(sagemaker_session=pipeline_session),
        description="local pipeline example"
    )

    ## pipeline will execute locally
    print('\t\t => Starting pipeline exection')
    execution = pipeline.start()

    steps = execution.list_steps()
    print(f'\t\t => Pipeline steps: {steps}')

    ## FIXME
    training_job_name = steps['PipelineExecutionSteps'][0]['Metadata']['TrainingJob']['Arn']

    step_outputs = pipeline_session.sagemaker_client.describe_training_job(TrainingJobName=training_job_name)
