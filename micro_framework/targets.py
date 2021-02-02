import asyncio
import inspect
from functools import partial
from typing import Union, Callable, Dict

from micro_framework.dependencies import Dependency, RunnerDependency
from micro_framework.extensions import Extension


async def call_dependencies(
        dependencies: Dict, action: str, *args, **kwargs) -> Dict:
    futures = []
    names = []
    for name, dependency in dependencies.items():
        method = getattr(dependency, action)
        futures.append(method(*args, **kwargs))
        names.append(name)

    results = await asyncio.gather(*futures)
    ret = {name: result for name, result in zip(names, results)}
    return ret


class TargetExecutor:
    def __init__(self, config, target, dependencies, runner_dependencies):
        self._config = config
        self.dependencies = dependencies
        self.runner_dependencies = runner_dependencies or {}
        self.target = target

    async def pre_call(self):
        await call_dependencies(
            self.dependencies, "setup_dependency", self
        )
        self.injected_dependencies = await call_dependencies(
            self.dependencies, "get_dependency", self
        )

    async def after_call(self):
        await call_dependencies(
            self.dependencies, "after_call", self, self.result,
            self.exception
        )
        if self.exception:
            raise self.exception

    async def run_async(self, *args, **kwargs):
        """
        Run the async target function/method.

        :return : Returns the target result
        :raises: An Exception raised from the target can be raised.
        """
        await self.pre_call()
        kwargs.update(**self.runner_dependencies) # Already loaded from runner
        kwargs.update(**self.injected_dependencies)

        self.exception = None
        self.result = None
        try:
            self.result = await self.target(*args, **kwargs)
        except Exception as exc:
            self.exception = exc
        finally:
            await self.after_call()
        return self.result

    def run(self, *args, **kwargs):
        """
        Run the target function/method synchronously.

        This method is called when the worker has an executor to call the
        target.

        If the target is a coroutine and the worker_mode is asyncio,
        then run_async should be called instead.

        :return : Returns the target result
        :raises: An Exception raised from the target can be raised.
        """
        event_loop = asyncio.get_event_loop()
        asyncio.run_coroutine_threadsafe(
            self.pre_call(),
            event_loop
        ).result()

        kwargs.update(**self.runner_dependencies)
        kwargs.update(**self.injected_dependencies)

        self.exception = None
        self.result = None
        try:
            self.result = self.target(*args, **kwargs)
        except Exception as exc:
            self.exception = exc
        finally:
            asyncio.run_coroutine_threadsafe(
                self.after_call(),
                event_loop
            ).result()

        return self.result

    @property
    def config(self):
        return self._config


class ClassTargetExecutor(TargetExecutor):
    def __init__(self, config, class_instance, target_method,
                 class_dependencies, target_runner_dependencies,
                 target_dependencies):
        super(ClassTargetExecutor, self).__init__(
            config, target_method, target_runner_dependencies,
            target_dependencies
        )
        self.class_instance = class_instance
        self.class_dependencies = class_dependencies

    async def pre_call(self):
        await call_dependencies(
            self.class_dependencies, "setup_dependency", self
        )
        injected_cls_dependencies = await call_dependencies(
            self.class_dependencies, "get_dependency", self
        )
        for name, dep in injected_cls_dependencies.items():
            setattr(self.class_instance, name, dep)
        return await super(ClassTargetExecutor, self).pre_call()

    async def after_call(self):
        await call_dependencies(
            self.class_dependencies, "after_call", self, self.result,
            self.exception
        )
        return await super(ClassTargetExecutor, self).after_call()



class Target(Extension):
    """
    Represents a Function to be called by a Worker.

    This Target controls the Dependencies that are going to be called and
    injected.

    After all setup, the worker will call its mount_target method, passing the
    worker instance. This method will return a TargetExecutor with the target
    RunnerDependencies injected and the Dependencies to be injected.

    """
    def __init__(self, target):
        self._runner_dependencies = {}
        self.all_dependencies = {}
        self._target = target
        self.populate_dependencies()

    def populate_dependencies(self):
        self.all_dependencies = getattr(self.target, "__dependencies__", {})
        self._dependencies = {}
        self._runner_dependencies = {}
        for name, dependency in self.all_dependencies.items():
            if isinstance(dependency, RunnerDependency):
                # RunnerDependencies are handled outside the worker.
                self._runner_dependencies[name] = dependency
            else:
                # Dependencies are handled inside the worker
                self._dependencies[name] = dependency

        self._target = getattr(self.target, "__original_function", self.target)

    async def call_dependencies(
            self, dependencies: Dict, action: str, *args, **kwargs) -> Dict:
        futures = []
        names = []
        for name, dependency in dependencies.items():
            method = getattr(dependency, action)
            futures.append(method(*args, **kwargs))
            names.append(name)

        results = await asyncio.gather(*futures)
        ret = {name: result for name, result in zip(names, results)}
        return ret

    async def on_worker_setup(self, worker):
        return await self.call_dependencies(
            self._runner_dependencies, "setup_dependency", worker
        )

    async def mount_target(self, worker):
        """
        Prepare this target by returning a TargetExecutor.
        """
        dependencies = await self.call_dependencies(
            self._runner_dependencies, "get_dependency", worker
        )
        executor_instance = TargetExecutor(
            self.runner.config, self.target,
            dependencies, self._dependencies
        )
        return executor_instance

    async def on_worker_finished(self, worker):
        return await self.call_dependencies(
            self._runner_dependencies, "after_call", worker, worker.result,
            worker.exception
        )

    async def target_detail(self):
        """
        Return the Details of an entrypoint's target.
        :param Entrypoint entrypoint: Entrypoint
        :return dict: Entrpoint Details dict.
        """
        target_id = str(self)

        target_parameters = self.parameters

        args = [
            name for name, parameter in target_parameters.items() if
            parameter.kind in [
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.VAR_POSITIONAL
            ] and parameter.default is inspect._empty  # arguments with
            # default are considered POSITIONAL_OR_KEYWORD. So here I'll
            # filter only those that don't have a default value.
        ]
        kwargs = [
            name for name, parameter in target_parameters.items() if
            parameter.kind in [
                inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.VAR_KEYWORD,
                inspect.Parameter.POSITIONAL_OR_KEYWORD
            ] and (parameter.default is not inspect._empty or
                   parameter.kind is inspect.Parameter.VAR_KEYWORD)
        ]
        return {
            'target': target_id,
            'args': args,
            'kwargs': kwargs,
            'docstring': inspect.getdoc(self.target)
        }

    @property
    def target(self) -> Callable:
        return self._target

    @property
    def parameters(self):
        """
        Returns the Parameters that the method accepts. (Excluding the
        Injected Dependencies)
        :return:
        """
        return {
            name: param
            for name, param in inspect.signature(self.target).parameters.items()
            if name not in self.all_dependencies
        }

    def __str__(self):
        return self.target.__name__

    def __repr__(self):
        return "<Target {}>".format(self.target.__name__)


class TargetFunction(Target):
    def bind(self, runner, parent=None):
        ext = super(TargetFunction, self).bind(runner, parent=parent)
        for name, dependency in ext._dependencies.items():
            extension = ext.bind_new_extension(
                name, dependency, runner, ext
            )
            ext._dependencies[name] = extension

        for name, dependency in ext._runner_dependencies.items():
            extension = ext.bind_new_extension(
                name, dependency, runner, ext
            )
            ext._runner_dependencies[name] = extension

        return ext


class TargetClassMethod(Target):
    """
    Represents a class to be instanced and then have a specific method
    be called by a Worker.

    It lists all dependencies to be injected into the class and into the method.
    """

    def __init__(self, target_class, target_method):
        self.target_class = target_class
        self.target_method = target_method
        method = getattr(target_class, target_method)
        super(TargetClassMethod, self).__init__(method)
        # The Superclass will handle all method handling, here we will extend
        # it to the class.
        self._class_runner_dependencies = {}
        self.populate_class_dependencies()

    def populate_class_dependencies(self):
        dependencies = inspect.getmembers(
            self.target_class, lambda x: isinstance(x, Dependency)
        )
        self._class_dependencies = {}
        for name, dependency in dependencies:
            if isinstance(dependency, RunnerDependency):
                self._class_runner_dependencies[name] = dependency
            else:
                self._class_dependencies[name] = dependency

    def bind(self, runner, parent=None):
        ext = super(TargetClassMethod, self).bind(runner, parent=parent)

        for name, dependency in ext._class_dependencies.items():
            extension = ext.bind_new_extension(
                name, dependency, runner, ext
            )
            ext._class_dependencies[name] = extension

        for name, dependency in ext._class_runner_dependencies.items():
            extension = ext.bind_new_extension(
                name, dependency, runner, ext
            )
            ext._class_runner_dependencies[name] = extension

        return ext

    async def on_worker_setup(self, worker):
        await super(TargetClassMethod, self).on_worker_setup(worker)
        return await self.call_dependencies(
            self._class_runner_dependencies, "setup_dependency", worker
        )

    async def mount_target(self, worker):
        cls = await self.get_class_instance(worker)
        dependencies = await self.call_dependencies(
            self._runner_dependencies, "get_dependency", worker
        )
        return ClassTargetExecutor(
            self.runner.config, cls, getattr(cls, self.target_method),
            self._class_dependencies, dependencies,
            self._dependencies
        )

    async def on_worker_finished(self, worker):
        await super(TargetClassMethod, self).on_worker_finished(worker)
        return await self.call_dependencies(
            self._class_runner_dependencies, "after_call", worker,
            worker.result, worker.exception
        )

    async def get_class_instance(self, worker):
        injected_cls_runner_dependencies = await self.call_dependencies(
            self._class_runner_dependencies, "get_dependency", worker
        )
        cls = self.target_class()
        for name, dependency in injected_cls_runner_dependencies.items():
            setattr(cls, name, dependency)
        return cls

    def __str__(self):
        return "{}.{}".format(
            self.target_class.__name__, self.target_method
        )

    def __repr__(self):
        return "<TargetClassMethod {}.{}>".format(
            self.target_class.__name__, self.target_method
        )


def new_target(target: Callable, method_name=None) -> Union[
    TargetFunction, TargetClassMethod]:
    """
    Instantiates a Target based on the parameters given.

    If the target parameter is a class, then we return a TargetClassMethod
    class.

    Else we return a TargetFunction class.

    :param callable target: A Function or Class
    :param str method_name: If a class is given, then this will be the method
    to be called.
    :return Union[TargetFunction, TargetClassMethod]: The Instantiated Target.
    """
    if inspect.isclass(target) and not method_name:
        method_name = "__call__"
    elif not inspect.isclass(target):
        method_name = None

    if method_name:
        return TargetClassMethod(target, method_name)

    return TargetFunction(target)
