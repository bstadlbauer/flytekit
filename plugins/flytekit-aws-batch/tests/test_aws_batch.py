from flytekitplugins.awsbatch import AWSBatch

from flytekit import PythonFunctionTask, task
from flytekit.extend import Image, ImageConfig, SerializationSettings


def test_spark_task():
    @task(task_config=AWSBatch())
    def mapper(a: int) -> str:
        inc = a + 2
        return str(inc)

    assert mapper.task_config is not None
    assert mapper.task_config == AWSBatch()
    assert isinstance(mapper, PythonFunctionTask)

    default_img = Image(name="default", fqn="test", tag="tag")
    settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )

    assert mapper.get_config(settings) == {}
