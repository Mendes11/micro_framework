import asyncio

from micro_framework.config import FrameworkConfig
from micro_framework.dependencies import RunnerDependency, Dependency


def is_runner_dependency(obj):
    return isinstance(obj, RunnerDependency)

def is_dependency(obj):
    return isinstance(obj, Dependency)


async def get_dependency_standalone(provider: Dependency, worker_cls=None,
                                    **configurations):
    """
    Returns an injected framework Dependency outside the scope of the
    Framework.

    It creates a FrameworkConfig from the given configurations (and default
    ones defined by the framework itself). This configuration is used to
    instantiate a worker, whose class is either given by the caller or
    MockWorker bellow is used (which only stores config attribute).

    :param Dependency provider: The Dependency Provider
    :param worker_cls: Worker Class to instantiate with configurations
    :param configurations: Additional Configurations to use in FrameworkConfig
    :return: Injected Callable
    """
    config = FrameworkConfig(configurations)
    provider.config = config

    class MockWorker:
        def __init__(self, config):
            self.config = config

    worker_cls = worker_cls or MockWorker
    worker = worker_cls(config)
    await provider.setup()
    await provider.setup_dependency(worker)
    return await provider.get_dependency(worker)


def get_dependency_standalone_sync(provider: Dependency, worker_cls=None,
                                   **configurations):
    """
    Do the same as :method:get_dependency_standalone but by creating a
    temporary event loop prior to calling it.


    :param Dependency provider: The Dependency Provider
    :param worker_cls: Worker Class to instantiate with configurations
    :param configurations: Additional Configurations to use in FrameworkConfig
    :return: Injected Callable
    """
    loop = asyncio.new_event_loop()
    return loop.run_until_complete(
        get_dependency_standalone(
            provider, worker_cls=worker_cls, **configurations
        )
    )
