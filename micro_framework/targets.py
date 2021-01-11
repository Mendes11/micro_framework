import asyncio
import inspect
from functools import partial
from typing import Union, Callable, Dict

from micro_framework.dependencies import Dependency
from micro_framework.extensions import Extension


class Target():
    """
    Represents a Function to be called by a Worker.

    This Target controls the Dependencies that are going to be called and
    injected.

    After all setup, the worker will call its mount_target method, passing the
    worker instance. This method will return a partial with the target
    dependencies injected.

    """
    def __init__(self, target):
        self._dependencies = {}
        self._target = target
        self.populate_dependencies()

    def populate_dependencies(self):
        self._dependencies = getattr(self.target, "__dependencies__", {})
        self._target = getattr(self.target, "__original_function", self.target)
        # fn_parameters = inspect.signature(self.target).parameters
        # for name, parameter in fn_parameters.items():
        #     annotation = parameter.annotation
        #     if isinstance(annotation, Dependency):
        #         self._dependencies[name] = annotation

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
            self._dependencies, "setup_dependency", worker
        )

    async def mount_target(self, worker):
        """
        Prepare this target by returning a callable with all dependency
        arguments already loaded in a partial.
        """
        dependencies = await self.call_dependencies(
            self._dependencies, "get_dependency", worker
        )
        return partial(self.target, **dependencies)

    async def on_worker_finished(self, worker):
        return await self.call_dependencies(
            self._dependencies, "after_call", worker, worker.result,
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
            if name not in self._dependencies
        }

    def __str__(self):
        return self.target.__name__

    def __repr__(self):
        return "<Target {}>".format(self.target.__name__)


class TargetFunction(Target, Extension):
    async def bind(self, runner, parent=None):
        ext = await super(TargetFunction, self).bind(runner, parent=parent)
        for name, dependency in ext._dependencies.items():
            extension = await ext.bind_new_extension(
                name, dependency, runner, ext
            )
            ext._dependencies[name] = extension
        return ext


class TargetClassMethod(Extension, Target):
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
        self._class_dependencies = {}
        self.populate_class_dependencies()

    def populate_class_dependencies(self):
        dependencies = inspect.getmembers(
            self.target_class, lambda x: isinstance(x, Dependency)
        )
        for name, dependency in dependencies:
            self._class_dependencies[name] = dependency

    async def bind(self, runner, parent=None):
        ext = await super(TargetClassMethod, self).bind(runner, parent=parent)

        for name, dependency in ext._dependencies.items():
            extension = await ext.bind_new_extension(
                name, dependency, runner, ext
            )
            ext._class_dependencies[name] = extension

        for name, dependency in ext._class_dependencies.items():
            extension = await ext.bind_new_extension(
                name, dependency, runner, ext
            )
            ext._class_dependencies[name] = extension

        return ext

    async def on_worker_setup(self, worker):
        await super(TargetClassMethod, self).on_worker_setup(worker)
        return await self.call_dependencies(
            self._class_dependencies, "setup_dependency", worker
        )

    async def mount_target(self, worker):
        cls = await self.get_class_instance(worker)
        dependencies = await self.call_dependencies(
            self._dependencies, "get_dependency", worker
        )
        method = getattr(cls, self.target_method)
        return partial(method, **dependencies)

    async def on_worker_finished(self, worker):
        await super(TargetClassMethod, self).on_worker_finished(worker)
        return await self.call_dependencies(
            self._class_dependencies, "after_call", worker, worker.result,
            worker.exception
        )

    async def get_class_instance(self, worker):
        class_dependencies = await self.call_dependencies(
            self._class_dependencies, "get_dependency", worker
        )
        cls = self.target_class()
        for name, dependency in class_dependencies.items():
            setattr(cls, name, dependency)
        return cls

    def __str__(self):
        return "{}.{}".format(
            self.target_class.__name__.lower(), self.target_method
        )

    def __repr__(self):
        return "<TargetClassMethod {}.{}>".format(
            self.target_class.__name__.lower(), self.target_method
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
