import os
import typing
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from flytekit import FlyteContextManager, PythonFunctionTask, TaskMetadata
from flytekit.core.map_task import MapPythonTask
from flytekit.extend import ExecutionState, SerializationSettings, TaskPlugins
from flytekit.models.array_job import ArrayJob


@dataclass
class AWSBatch(object):
    """
    Use this to configure a job definition for a AWS batch job. Task's marked with this will automatically execute
    natively onto AWS batch service

    Args:
        job_definition: Dictionary of job definition. The variables should match what AWS batch expects
        concurrency: If specified, this limits the number of mapped tasks than can run in parallel to the given batch size
        min_success_ratio: If specified, this determines the minimum fraction of total jobs which can complete
        successfully before terminating this task and marking it successful.
    """

    job_definition: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if self.job_definition is None:
            self.job_definition = {}


class AWSBatchFunctionTask(PythonFunctionTask):
    """
    Actual Plugin that transforms the local python code for execution within AWS batch job
    """

    _AWS_BATCH_TASK_TYPE = "aws-batch"

    def __init__(self, task_config: AWSBatch, task_function: Callable, **kwargs):
        if task_config is None:
            task_config = AWSBatch()
        super(AWSBatchFunctionTask, self).__init__(
            task_config=task_config,
            task_type=self._AWS_BATCH_TASK_TYPE,
            task_function=task_function,
            **kwargs
        )

    def get_config(self, settings: SerializationSettings) -> Dict[str, str]:
        return self.task_config.job_definition


# Inject the AWS batch plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(AWSBatch, AWSBatchFunctionTask)
