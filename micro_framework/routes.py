from importlib import import_module

from micro_framework.exceptions import ExtensionIsStopped
from micro_framework.extensions import Extension
from micro_framework.workers import Worker


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

    def __init__(self, function_path, dependencies=None, translators=None,
                 worker_class=Worker):
        self._dependencies = dependencies or {}
        self._translators = translators or []
        self._function_path = function_path
        self.stopped = False
        self.current_workers = {}
        self.entrypoint = None
        self.worker_class = worker_class

    def route_result(self, future):
        if future.exception():
            # Unhandled exception
            raise future.exception()
        worker = future.result()
        if worker.exception:
            self.entrypoint.on_failed_route(worker)
        self.entrypoint.on_success_route(worker)

    def get_worker_instance(self, entry_id, *fn_args, **fn_kwargs):
        return self.worker_class(
            entry_id, self.function_path, self.dependencies, self.translators,
            *fn_args, **fn_kwargs
        )

    def start_route(self, entry_id, *fn_args, **fn_kwargs):
        if self.stopped:
            raise ExtensionIsStopped()
        worker = self.get_worker_instance(entry_id, *fn_args, **fn_kwargs)
        future = self.runner.spawn_worker(worker)
        # Before adding a callback to the future obj we register it
        self.current_workers[future] = worker
        future.add_done_callback(self.route_result)
        return worker

    def bind(self, runner):
        super(Route, self).bind(runner)
        if self.dependencies:
            for name, dependency in self.dependencies.items():
                dependency.bind(runner)

    def bind_entrypoint(self, entrypoint):
        self.entrypoint = entrypoint

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
