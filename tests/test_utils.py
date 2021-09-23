from typing import Callable
from mock import AsyncMock, MagicMock

import pytest

from micro_framework.config import FrameworkConfig
from micro_framework.dependencies import Dependency, RunnerDependency
from micro_framework.targets import ExecutorsType
from micro_framework.utils import is_dependency, is_runner_dependency, \
    get_dependency_standalone, get_dependency_standalone_sync

pytestmark = pytest.mark.asyncio


class MockDependency(Dependency):
    async def get_dependency(self, worker: 'ExecutorsType') -> Callable:
        return lambda x: f'this is a Dependency - {x}'

class MockRunnerDependency(RunnerDependency):
    async def get_dependency(self, worker: 'ExecutorsType') -> Callable:
        return lambda x: f'this is a RunnerDependency - {x}'


def test_is_dependency():
    assert is_dependency(MockDependency())

def test_runner_dependency_is_dependency():
    assert is_dependency(RunnerDependency())


def test_dependency_is_not_runner_dependency():
    assert not is_runner_dependency(MockDependency())

def test_is_runner_dependency():
    assert is_runner_dependency(MockRunnerDependency())


async def test_get_dependency_standalone():
    dep = await get_dependency_standalone(MockDependency())
    assert dep(31) == "this is a Dependency - 31"

async def test_get_dependency_standalone_runner_dep():
    dep = await get_dependency_standalone(MockRunnerDependency())
    assert dep(2) == "this is a RunnerDependency - 2"

async def test_dependency_with_configs():
    provider = AsyncMock()
    await get_dependency_standalone(provider, test1=1, test2=2)

    provider.get_dependency.assert_called_once()
    worker = provider.get_dependency.call_args[0][0]
    assert hasattr(worker, "config")
    assert worker.config["test1"] == 1
    assert worker.config["test2"] == 2


async def test_dependency_worker_class():
    provider = AsyncMock()
    worker_cls = MagicMock()

    await get_dependency_standalone(
        provider, worker_cls=worker_cls, test="test"
    )

    worker_cls.assert_called_with(FrameworkConfig({"test": "test"}))


def test_get_dependency_sync():
    provider = MockDependency()
    dep = get_dependency_standalone_sync(provider)
    assert dep("something") == "this is a Dependency - something"
