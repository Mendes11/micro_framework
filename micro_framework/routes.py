from importlib import import_module

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

    def __init__(self, function_path, dependencies=None, translators=None):
        self._dependencies = dependencies or {}
        self._translators = translators or []
        self._function_path = function_path
        self.runner = None
        self.stopped = False
        self.current_workers = {}

    def get_callable(self):
        *module, function = self.function_path.split('.')
        function = getattr(import_module('.'.join(module)), function)
        return function

    def call_dependencies(self, action, *args, **kwargs):
        ret = {}
        for name, dependency in self._dependencies.items():
            ret[name] = getattr(dependency, action)(*args, **kwargs)
        return ret

    def route_result(self, future):
        exception = future.exception()
        if exception:
            print("Exception")
            # self.on_failure(exception, **kwargs)
            raise exception
        print("Success")
        print(future.test)
        # self.on_success(future.result(), **kwargs)

    def start_route(
            self, success_callback, failure_callback, *fn_args, **fn_kwargs
    ):
        if self.stopped:
            raise ExtensionIsStopped()
        worker = self.as_worker()
        future = self.runner.spawn_worker(
            worker,
            *fn_args,
            **fn_kwargs
        )
        self.current_workers[future] = {
            'worker': worker,
            'success_callback': success_callback,
            'failure_callback': failure_callback
        }
        future.add_done_callback(self.route_result)
        return worker

    def run(self, *args, **kwargs):
        self.function = self.get_callable()
        self.call_dependencies('setup')
        self.injected_dependencies = self.call_dependencies(
            'get_dependency', self
        )
        result = None
        self.fn_args = args
        self.fn_kwargs = kwargs
        for translator in self._translators:
            args, kwargs = translator.translate(*args, **kwargs)

        try:
            fn_kwargs = {**kwargs, **self.dependencies}  # Updating kwargs
            result = self.function(*args, **fn_kwargs)
        except Exception as exc:
            self.call_dependencies('after_call', result, exc, self)
            raise
        self.call_dependencies('after_call', result, None, self)
        return result

    def as_worker(self):
        unbinded_instance = type(self)(
            *self.__params[0], **self.__params[1]
        )
        return unbinded_instance

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
