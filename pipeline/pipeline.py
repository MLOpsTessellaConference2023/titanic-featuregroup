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
from sagemaker.workflow.pipeline_context import LocalPipelineSession
from sagemaker.workflow.steps import Step, StepTypeEnum
from sagemaker.workflow.pipeline import Pipeline

pipeline_session = LocalPipelineSession()

steps = [
    Step(
        name="WrangleData",
        step_type=StepTypeEnum.PROCESSING,
    ),
    Step(
        name="FeatureEngineering",
        step_type=StepTypeEnum.PROCESSING,
    ),
    Step(
        name="FeatureValidation",
        step_type=StepTypeEnum.PROCESSING,
    ),
    Step(
        name="IngestFeatureGroup",
        step_type=StepTypeEnum.PROCESSING,
    ),
]

pipeline = Pipeline(
    name="titanic-featuregroup",
    steps=steps,
    sagemaker_session=pipeline_session
)

pipeline.create(
    role_arn=sagemaker.get_execution_role(),
    description="local pipeline example"
)

## pipeline will execute locally
execution = pipeline.start()

steps = execution.list_steps()

## FIXME
training_job_name = steps['PipelineExecutionSteps'][0]['Metadata']['TrainingJob']['Arn']

step_outputs = pipeline_session.sagemaker_client.describe_training_job(TrainingJobName=training_job_name)
