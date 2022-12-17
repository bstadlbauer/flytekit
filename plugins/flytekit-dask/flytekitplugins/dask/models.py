from typing import Optional

from flyteidl.plugins import dask_pb2 as _dask_task

from flytekit.models import common as _common
from flytekit.models import task as _task


class Scheduler(_common.FlyteIdlEntity):
    """
    Configuration for the scheduler pod

    :param image: Optional image to use.
    :param resources: Optional resources to use.
    """

    def __init__(self, image: Optional[str], resources: Optional[_task.Resources]):
        self._image = image
        self._resources = resources

    @property
    def image(self) -> Optional[str]:
        """
        :return: The optional image for the scheduler pod
        """
        return self._image

    @property
    def resources(self) -> Optional[_task.Resources]:
        """
        :return: Optional resources for the scheduler pod
        """
        return self._resources

    def to_flyte_idl(self) -> _dask_task.Scheduler:
        """
        :return: The scheduler spec serialized to protobuf
        """
        return _dask_task.Scheduler(
            image=self.image,
            resources=self.resources.to_flyte_idl() if self.resources else None,
        )


class WorkerGroup(_common.FlyteIdlEntity):
    """
    Configuration for a dask worker group

    :param image: Optional image to use for the pods of the worker group
    :param number_of_workers: Optional number of workers in the group
    :param resources: Optional resources to use for the pods of the worker group
    """

    def __init__(self, number_of_workers: Optional[int], image: Optional[str], resources: Optional[_task.Resources]):
        self._number_of_workers = number_of_workers
        self._image = image
        self._resources = resources

    @property
    def number_of_workers(self) -> Optional[int]:
        """
        :return: Optional number of workers for the worker group
        """
        return self._number_of_workers

    @property
    def image(self) -> Optional[str]:
        """
        :return: The optional image to use for the worker pods
        """
        return self._image

    @property
    def resources(self) -> Optional[_task.Resources]:
        """
        :return: Optional resources to use for the worker pods
        """
        return self._resources

    def to_flyte_idl(self) -> _dask_task.WorkerGroup:
        """
        :return: The dask cluster serialized to protobuf
        """
        return _dask_task.WorkerGroup(
            number_of_workers=self.number_of_workers,
            image=self.image,
            resources=self.resources.to_flyte_idl() if self.resources else None,
        )


class DaskJob(_common.FlyteIdlEntity):
    """
    Configuration for the custom dask job to run

    :param scheduler: Configuration for the scheduler
    :param dask_cluster: Configuration for the dask cluster
    """

    def __init__(self, scheduler: Scheduler, workers: WorkerGroup):
        self._scheduler = scheduler
        self._workers = workers

    @property
    def scheduler(self) -> Scheduler:
        """
        :return: Configuration for the scheduler pod
        """
        return self._scheduler

    @property
    def workers(self) -> WorkerGroup:
        """
        :return: Configuration of the default worker group
        """
        return self._workers

    def to_flyte_idl(self) -> _dask_task.DaskJob:
        """
        :return: The dask job serialized to protobuf
        """
        return _dask_task.DaskJob(
            scheduler=self.scheduler.to_flyte_idl(),
            workers=self.workers.to_flyte_idl(),
        )
