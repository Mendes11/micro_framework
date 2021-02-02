import sys

from functools import wraps
from typing import Any, Callable

from micro_framework.extensions import Extension

from micro_framework.workers import Worker

def inject(**dependencies):
    for dep in dependencies.values():
        if not isinstance(dep, Dependency):
            raise TypeError(
                f"Injected Dependency {type(dep)} should be a subclass of "
                f"{Dependency}"
            )
    def decorator(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            return fn(*args, **kwargs)
        setattr(sys.modules[fn.__module__], fn.__name__, wrapped)
        wrapped.__dependencies__ = dependencies
        return wrapped
    return decorator


class Dependency(Extension):
    """
    Kind of extension that injects a dependency into the worker target.

    The Dependency instance will be used by multiple workers so it is not
    recommended to store states to be used between the methods calls unless
    it is desirable to share this states between all workers of the same
    Route.

    If you wish to store some values to be used between the methods in the
    same worker, use the worker instance passed.

    The Dependency Life-Cycle is:

    ###### Main Process/Thread #########

    1. setup:
        . Like any Extension, it has a setup method. Usually not used for
        this kind of Dependency.

    2. Setup/Get RunnerDependencies and inject them in the worker call

    ###### Worker Process/Thread ########

    3. setup_dependency:
        .Do anything that might be needed before actually creating the
        injected dependency.

    4. get_dependency:
        .Returns the dependency to be injected

    5. Run the Target Function/Method passing the injected dependency

    6. after_call:
        .After the worker is executed, we call this method passing the
        worker and the result + exception from that call.

    ###### Main Process/Thread #########

    7. after_call of RunnerDependency classes.

    """
    picklable = True

    def bind(self, runner, parent=None):
        ext = super(Dependency, self).bind(runner, parent=None)
        ext.config = runner.config
        return ext

    async def setup_dependency(self, worker: Worker):
        """
        Do any setup necessary before the dependency injection.
        """
        pass

    async def get_dependency(self, worker: Worker) -> Callable:
        """
        Injects the dependency that will be passed to the Target Function.
        """
        pass

    async def after_call(self, worker: Worker, result: Any, exc: Exception):
        """
        Cleanup after the function has finished or raised.
        """
        pass


class RunnerDependency(Dependency):
    """
    A type of Dependency that is loaded before sending the task to the
    executor.

    This Dependency has access to the runner instance and therefore can spawn
    extensions or run asyncio coroutines inside the runner event-loop.

    The user should avoid this type of Dependency when working with Processes,
    since it can cause some pickling problems, due to the injected dependency
    being sent before the spawning of the worker instead of after like the
    Dependency class.

    The RunnerDependency Life-Cycle is:

    ###### Main Process/Thread #########

    1. setup:
        . Since this kind of dependency has access to the runner, we might
        setup some kind of internal worker here.

    2. start:
        . Start the RunnerDependency internals

    3. setup_dependency:
        .Do anything that might be needed before actually creating the
        injected dependency.

    4. get_dependency:
        .Returns the dependency to be injected

    ###### Worker Process/Thread ########

    5. setup/get_dependency of normal Dependency classes

    6. Run the Target Function/Method with injected dependencies

    7. after_call of normal Dependency classes

    ###### Main Process/Thread #########

    8. after_call:
        .After the worker is executed, we call this method passing the
        worker and the result + exception from that call.

    """
    picklable = False

    def bind(self, runner, parent=None):
        ext = super(Dependency, self).bind(runner, parent=parent)
        ext.target = parent
        ext.config = runner.config
        return ext

    async def setup_dependency(self, worker: Worker):
        """
        Do any setup necessary before the dependency injection.
        """
        pass

    async def get_dependency(self, worker: Worker) -> Callable:
        """
        Injects the dependency that will be passed to the Target Function.

        * NOTE:
            Remember that this will be called before spawning a worker
            and there are restrictions.

            Currently we use loky as a process spawner and it uses
            cloudpickle, so search about it's limitations.
        """
        pass

    async def after_call(self, worker: Worker, result: Any, exc: Exception):
        """
        Cleanup after the function has finished or raised.
        """
        pass


class Config(Dependency):
    """
    Returns the Runner Configuration.
    """
    async def get_dependency(self, worker: Worker):
        return self.config
