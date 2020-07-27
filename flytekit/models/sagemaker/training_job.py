from __future__ import absolute_import, annotations

from typing import List
from flyteidl.plugins.sagemaker import training_job_pb2 as _training_job_pb2
from flytekit.models import common as _common


class TrainingJobResourceConfig(_common.FlyteIdlEntity):
    def __init__(
            self,
            instance_count: int,
            instance_type: str,
            volume_size_in_gb: int,
    ):
        self._instance_count = instance_count
        self._instance_type = instance_type
        self._volume_size_in_gb = volume_size_in_gb

    @property
    def instance_count(self: TrainingJobResourceConfig) -> int:
        """
        :rtype: int
        """
        return self._instance_count

    @property
    def instance_type(self: TrainingJobResourceConfig) -> str:
        """
        :rtype: str
        """
        return self._instance_type

    @property
    def volume_size_in_gb(self: TrainingJobResourceConfig) -> int:
        """
        :rtype: int
        """
        return self._volume_size_in_gb

    def to_flyte_idl(self: TrainingJobResourceConfig) -> _training_job_pb2.TrainingJobResourceConfig:
        """

        :rtype: _training_job_pb2.TrainingJobResourceConfig
        """
        return _training_job_pb2.TrainingJobResourceConfig(
            instance_count=self.instance_count,
            instance_type=self.instance_type,
            volume_size_in_gb=self.volume_size_in_gb,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object: _training_job_pb2.TrainingJobResourceConfig) -> TrainingJobResourceConfig:
        """

        :param pb2_object:
        :rtype: TrainingJobResourceConfig
        """
        return cls(
            instance_count=pb2_object.instance_count,
            instance_type=pb2_object.instance_type,
            volume_size_in_gb=pb2_object.volume_size_in_gb,
        )


class MetricDefinition(_common.FlyteIdlEntity):
    def __init__(
            self: MetricDefinition,
            name: str,
            regex: str,
    ):
        self._name = name
        self._regex = regex

    @property
    def name(self: MetricDefinition) -> str:
        """

        :rtype: str
        """
        return self._name

    @property
    def regex(self: MetricDefinition) -> str:
        """

        :rtype: str
        """
        return self._regex

    def to_flyte_idl(self: MetricDefinition) -> _training_job_pb2.MetricDefinition:
        """

        :rtype: _training_job_pb2.MetricDefinition
        """
        return _training_job_pb2.MetricDefinition(
            name=self.name,
            regex=self.regex,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object: _training_job_pb2.MetricDefinition) -> MetricDefinition:
        """

        :param pb2_object: _training_job_pb2.MetricDefinition
        :return: MetricDefinition
        """
        return cls(
            name=pb2_object.name,
            regex=pb2_object.regex,
        )


class InputMode(object):
    PIPE = _training_job_pb2.InputMode.PIPE
    FILE = _training_job_pb2.InputMode.FILE


class AlgorithmName(object):
    CUSTOM = _training_job_pb2.AlgorithmName.CUSTOM
    XGBOOST = _training_job_pb2.AlgorithmName.XGBOOST


class AlgorithmSpecification(_common.FlyteIdlEntity):
    def __init__(
            self,
            input_mode: int,
            algorithm_name: int,
            algorithm_version: str,
            metric_definitions: List[MetricDefinition],
    ):
        self._input_mode = input_mode
        self._algorithm_name = algorithm_name
        self._algorithm_version = algorithm_version
        self._metric_definitions = metric_definitions

    @property
    def input_mode(self: AlgorithmSpecification) -> int:
        """
        enum value from InputMode
        :rtype: int
        """
        return self._input_mode

    @property
    def algorithm_name(self: AlgorithmSpecification) -> int:
        """
        enum value from AlgorithmName
        :rtype: int
        """
        return self._algorithm_name

    @property
    def algorithm_version(self: AlgorithmSpecification) -> str:
        """
        version of the algorithm (if using built-in algorithm mode)
        :rtype: str
        """
        return self._algorithm_version

    @property
    def metric_definitions(self: AlgorithmSpecification) -> List[MetricDefinition]:
        """

        :rtype: List[MetricDefinition]
        """
        return self._metric_definitions

    def to_flyte_idl(self: AlgorithmSpecification) -> _training_job_pb2.AlgorithmSpecification:

        return _training_job_pb2.AlgorithmSpecification(
            input_mode=self.input_mode,
            algorithm_name=self.algorithm_name,
            algorithm_version=self.algorithm_version,
            metric_definitions=[m.to_flyte_idl() for m in self.metric_definitions],
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object: _training_job_pb2.AlgorithmSpecification) -> AlgorithmSpecification:

        return cls(
            input_mode=pb2_object.input_mode,
            algorithm_name=pb2_object.algorithm_name,
            algorithm_version=pb2_object.algorithm_version,
            metric_definitions=[MetricDefinition.from_flyte_idl(m) for m in pb2_object.metric_definitions],
        )


class TrainingJob(_common.FlyteIdlEntity):
    def __init__(
            self,
            algorithm_specification: AlgorithmSpecification,
            training_job_resource_config: TrainingJobResourceConfig,
    ):
        self._algorithm_specification = algorithm_specification
        self._training_job_resource_config = training_job_resource_config

    @property
    def algorithm_specification(self: TrainingJob) -> AlgorithmSpecification:
        """
        :rtype: AlgorithmSpecification
        """
        return self._algorithm_specification

    @property
    def training_job_resource_config(self: TrainingJob) -> TrainingJobResourceConfig:
        """
        :rtype: TrainingJobResourceConfig
        """
        return self._training_job_resource_config

    def to_flyte_idl(self: TrainingJob) -> _training_job_pb2.TrainingJob:
        """
        :rtype: _training_job_pb2.TrainingJob
        """

        return _training_job_pb2.TrainingJob(
            algorithm_specification=self.algorithm_specification.to_flyte_idl(),
            training_job_resource_config=self.training_job_resource_config.to_flyte_idl(),
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object: _training_job_pb2.TrainingJob) -> TrainingJob:
        """

        :param pb2_object:
        :rtype: TrainingJob
        """
        return cls(
            algorithm_specification=pb2_object.algorithm_specification,
            training_job_resource_config=pb2_object.training_job_resource_config,
        )
