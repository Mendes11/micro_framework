import logging
from functools import partial

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

    def handle_finished_worker(self, entry_id, worker):
        self.entrypoint.on_finished_route(entry_id, worker)

    def worker_result(self, entry_id, future):
        logger.debug(f"{self} Received a worker result.")
        # Cleaning runner
        self.runner.spawned_workers.pop(future)
        if future.exception():
            # Unhandled exception propagate it to kill thread.
            raise future.exception()
        worker = future.result()
        self.handle_finished_worker(entry_id, worker)

    def get_worker_instance(self, *fn_args, **fn_kwargs):
        return self.worker_class(
            self.function_path, self.dependencies.copy(),
            self.translators.copy(), self.runner.config, *fn_args, **fn_kwargs
        )

    def run_worker(self, entry_id, worker, callback=None):
        if callback is None:
            callback = partial(self.worker_result, entry_id)
        future = self.runner.spawn_worker(worker)
        future.add_done_callback(callback)

    def start_route(self, entry_id, *fn_args, **fn_kwargs):
        if self.stopped:
            raise ExtensionIsStopped()
        worker = self.get_worker_instance(*fn_args, **fn_kwargs)
        self.run_worker(entry_id, worker)
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


class CallbackRoute(Route):
    def __init__(self, *args, callback_function_path, **kwargs):
        super(CallbackRoute, self).__init__(*args, **kwargs)
        self.callback_function_path = callback_function_path

    def handle_finished_callback_worker(self, entry_id, callback_worker):
        # TODO Check the best behavior here.
        return self.entrypoint.on_finished_route(
            entry_id, worker=callback_worker.original_worker
        )

    def callback_worker_result(self, entry_id, future):
        logger.debug(f"{self} received a callback worker result.")
        self.runner.spawned_workers.pop(future)
        if future.exception():
            raise future.exception()
        callback_worker = future.result()
        self.handle_finished_callback_worker(entry_id, callback_worker)

    def get_callback_worker_instance(self, *fn_args, **fn_kwargs):
        return self.worker_class(
            self.callback_function_path, self.dependencies.copy(),
            self.translators.copy(), self.runner.config, *fn_args, **fn_kwargs
        )

    def start_callback_route(self, entry_id, worker):
        extended_args = (*worker.fn_args, worker.exception)
        callback_worker = self.get_callback_worker_instance(
            *extended_args, **worker.fn_kwargs)
        callback_worker.original_worker = worker
        callback = partial(self.callback_worker_result, entry_id)
        self.run_worker(entry_id, callback_worker, callback=callback)
        return callback_worker

    def handle_finished_worker(self, entry_id, worker):
        if worker.exception and self.callback_function_path:
            logger.debug("Finished worker has an exception, calling callback "
                         "route.")
            return self.start_callback_route(entry_id, worker)
        super(CallbackRoute, self).handle_finished_worker(entry_id, worker)
