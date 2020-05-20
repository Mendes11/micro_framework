from functools import partial

from micro_framework.exceptions import ExtensionIsStopped
from micro_framework.extensions import Extension


class Route(Extension):
    """
    Base Extension Destined to link an endpoint to an entry function.

    Protocol-Specific routes should implement this class and extend its
    features.

    In order to keep the framework code decoupled from the to-be executed
    functions, we pass only the path of the function so it will be imported
    only when a new worker is spawned, making any setup from imports and etc
    (Django i'm looking to you) run in the spawned worker.
    """
    default_translator = []

    def __init__(self, function_path, dependencies, translators):
        self._dependencies = dependencies or {}
        self._translators = translators or self.default_translator
        self._function_path = function_path
        self.runner = None
        self.stopped = False

    def on_success(self, result, **callback_kwargs):
        pass

    def on_failure(self, exception, **callback_kwargs):
        pass

    def route_result(self, future, function_args, **kwargs):
        exception = future.exception()
        if exception:
            self.on_failure(exception, **kwargs)
            raise exception
        self.on_success(future.result(), **kwargs)

    def failure_route_result(self, future, function_args, **kwargs):
        pass

    def start_route(self, *fn_args, callback_kwargs=None):
        if self.stopped:
            raise ExtensionIsStopped()
        if callback_kwargs is None:
            callback_kwargs = {}
        callback = partial(
            self.route_result, function_args=fn_args, **callback_kwargs
        )
        future = self.runner.spawn_worker(
            route=self,
            callback=callback,
            fn_args=fn_args,
        )
        return future

    def bind(self, runner):
        super(Route, self).bind(runner)
        if self.dependencies:
            for name, dependency in self.dependencies.items():
                dependency.bind(runner)

    def stop(self):
        self.stopped = True

    @property
    def dependencies(self):
        return self._dependencies

    @property
    def function_path(self):
        return self._function_path

    @property
    def translators(self):
        return self._translators

    def __str__(self):
        return f'{self.__class__.__name__} -> {self.function_path}'

    def __repr__(self):
        return self.__str__()