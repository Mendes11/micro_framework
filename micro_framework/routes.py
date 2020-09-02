import logging

from functools import partial

from micro_framework.exceptions import ExtensionIsStopped
from micro_framework.extensions import Extension
from micro_framework.workers import Worker, CallbackWorker

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
    worker_class = Worker

    def __init__(self, target, entrypoint, dependencies=None,
                 translators=None, worker_class=None, backoff=None,
                 method_name=None):
        self._entrypoint = entrypoint
        self._dependencies = dependencies or {}
        self._translators = translators or []
        self._target = target
        self._method_name = method_name
        self.stopped = False
        self.current_workers = {}
        self.worker_class = worker_class or self.worker_class
        self._backoff = backoff

    def handle_finished_worker(self, entry_id, worker):

        if worker.exception and self.backoff and self.backoff.can_retry(worker):
            self.backoff.retry(worker)  # Retry and let the entrypoint finish
        self.entrypoint.on_finished_route(entry_id, worker)
        if worker.exception:
            raise worker.exception

    def worker_result(self, entry_id, future):
        logger.debug(f"{self} Received a worker result.")
        # Cleaning runner
        self.runner.spawned_workers.pop(future)
        if future.exception():
            # Unhandled exception propagate it to kill thread.
            raise future.exception()
        worker = future.result()
        self.handle_finished_worker(entry_id, worker)

    def get_worker_instance(self, *fn_args, _meta=None, **fn_kwargs):
        return self.worker_class(
            self.target, self.dependencies.copy(),
            self.translators.copy(), self.runner.config, *fn_args, _meta=_meta,
            method_name=self.method_name, **fn_kwargs
        )

    def run_worker(self, entry_id, worker, callback=None):
        if callback is None:
            callback = partial(self.worker_result, entry_id)
        future = self.runner.spawn_worker(worker)
        future.add_done_callback(callback)

    def start_route(self, entry_id, *fn_args, _meta=None, **fn_kwargs):
        if self.stopped:
            raise ExtensionIsStopped()
        worker = self.get_worker_instance(*fn_args, _meta=_meta, **fn_kwargs)
        self.run_worker(entry_id, worker)
        return worker

    def bind_to_extensions(self):
        self.entrypoint.bind_to_route(self)
        if self.dependencies:
            for name, dependency in self.dependencies.items():
                dependency.bind(self.runner.config) # TODO Maybe binding later to route only
        if self.backoff is not None:
            # Link this route to the given backoff class
            self.backoff.bind_route(self)

    def stop(self):
        self.stopped = True

    @property
    def dependencies(self):
        return self._dependencies

    @property
    def target(self):
        return self._target

    @property
    def method_name(self):
        return self._method_name

    @property
    def translators(self):
        return self._translators

    @property
    def entrypoint(self):
        return self._entrypoint

    @property
    def backoff(self):
        return self._backoff

    def __str__(self):
        return f'{self.__class__.__name__} -> {self.target}'

    def __repr__(self):
        return self.__str__()


class CallbackRoute(Route):
    callback_worker_class = CallbackWorker

    def __init__(self, *args, callback_target,
                 callback_worker_class=None, **kwargs):
        super(CallbackRoute, self).__init__(*args, **kwargs)
        self.callback_target = callback_target
        self.callback_worker_class = callback_worker_class or self.callback_worker_class

    def handle_finished_callback_worker(self, entry_id, callback_worker):
        self.entrypoint.on_finished_route(
            entry_id, worker=callback_worker.original_worker
        )
        if callback_worker.exception:
            raise callback_worker.exception

    def callback_worker_result(self, entry_id, future):
        logger.debug(f"{self} received a callback worker result.")
        self.runner.spawned_workers.pop(future)
        if future.exception():
            raise future.exception()
        callback_worker = future.result()
        self.handle_finished_callback_worker(entry_id, callback_worker)

    def get_callback_worker_instance(self, original_worker):
        return self.callback_worker_class(
            self.callback_target, original_worker
        )

    def start_callback_route(self, entry_id, worker):
        logger.info(f"{self.target} failed. Starting callback route.")
        callback_worker = self.get_callback_worker_instance(worker)
        callback = partial(self.callback_worker_result, entry_id)
        self.run_worker(entry_id, callback_worker, callback=callback)
        return callback_worker

    def handle_finished_worker(self, entry_id, worker):
        # Callback route will only be called if retry is not valid.
        if worker.exception and self.backoff and self.backoff.can_retry(worker):
            self.backoff.retry(worker)  # Retry and let the entrypoint finish
            return self.entrypoint.on_finished_route(entry_id, worker)
        elif worker.exception and self.callback_target:
            logger.debug(
                "Finished worker has an exception, calling callback route."
            )
            self.start_callback_route(entry_id, worker)
            raise worker.exception
        else:
            super(CallbackRoute, self).handle_finished_worker(entry_id, worker)
