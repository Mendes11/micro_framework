import sys

from functools import wraps
from typing import Any, Callable

from micro_framework.extensions import Extension

from micro_framework.workers import Worker


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

    2. setup_dependency:
        .Do anything that might be needed before actually creating the
        injected dependency.

    3. get_dependency:
        .Returns the dependency to be injected

    ###### Worker Process/Thread ########

    4. Run the Target Function/Method passing the injected dependency

    ###### Main Process/Thread #########

    5. Attribute the result/exception to the worker class

    6. after_call:
        .After the worker is executed, we call this method passing the
        worker and the result + exception from that call.
    """

    def bind(self, runner, parent=None):
        ext = super(Dependency, self).bind(runner, parent=None)
        ext.config = runner.config
        ext.target = parent
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


class Config(Dependency):
    """
    Returns the Runner Configuration.
    """
    async def get_dependency(self, worker: Worker):
        return worker.config


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


# TODO WorkerDependency, that works as the old dependencies and the methods
#  are called inside the worker context. Therefore it doesn't allow us to use
#  the runner. (Unbinded to the runner)


# TODO Also, since we cannot run the Target inside the worker, we should
#  mount the target with the pipeline to start the inner dependencies!

class WorkerDependency(Dependency):

    def bind(self, runner, parent=None):
        ext = super(Dependency, self).bind(runner, parent=None)
        ext.config = runner.config
        ext.target = parent
        return ext