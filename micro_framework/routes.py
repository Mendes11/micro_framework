import logging

from memory_profiler import profile

from micro_framework.exceptions import ExtensionIsStopped
from micro_framework.extensions import Extension
from micro_framework.workers import Worker

logger = logging.getLogger(__name__)


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
        logger.debug(f"{self} Received a worker result.")
        entry_id = self.current_workers.pop(future)
        # Cleaning runner
        self.runner.spawned_workers.pop(future)
        if future.exception():
            # Unhandled exception propagate it to kill thread.
            raise future.exception()
        worker = future.result()
        if entry_id is None:
            # This was already dealed with.
            logger.debug('Route result already treated.')
            return
        if worker.exception:
            return self.entrypoint.on_failed_route(entry_id, worker)
        return self.entrypoint.on_success_route(entry_id, worker)

    def get_worker_instance(self, *fn_args, **fn_kwargs):
        return self.worker_class(
            self.function_path, self.dependencies,
            self.translators, self.runner.config, *fn_args, **fn_kwargs
        )

    def start_route(self, entry_id, *fn_args, **fn_kwargs):
        if self.stopped:
            raise ExtensionIsStopped()
        worker = self.get_worker_instance(*fn_args, **fn_kwargs)
        try:
            future = self.runner.spawn_worker(worker)
        except Exception:
            return self.entrypoint.on_failure(entry_id)
        # Before adding a callback to the future obj we register it
        self.current_workers[future] = entry_id
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
