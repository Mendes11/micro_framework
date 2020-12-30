import inspect
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
                 method_name=None, metric_label=None):
        self._entrypoint = entrypoint
        self._dependencies = dependencies or {}
        self._translators = translators or []
        self._target = target
        self._method_name = method_name
        self.stopped = False
        self.current_workers = {}
        self.worker_class = worker_class or self.worker_class
        self._backoff = backoff
        self._metric_label = metric_label

    async def handle_finished_worker(self, entry_id, worker):
        """
        Override this method to do some validations after a worker has
        finished.

        This method should return either the worker instance or None.

        :param entry_id:
        :param worker:
        :return:
        """
        return worker

    async def run_backoff(self, entry_id, worker):
        """
        Run a backoff class.

        This method will return the worker from the retry.
        If None is returned, then it indicates that the backoff is running
        asynchronously and we let the entrypoint decide what to do.

        :param entry_id: Some object/identifier to be used to send a response
        for the correct caller.

        :param Worker worker: Worker Instance with result/exception attrs

        :return Optional[Worker]: Worker Instance with result/exception
        attrs or a None type, indicating an asynchronous backoff was made.
        """
        return await self.backoff.retry(worker)

    async def get_worker_instance(self, *fn_args, _meta=None, **fn_kwargs):
        return self.worker_class(
            self.target, self.dependencies.copy(),
            self.translators.copy(), self.runner.config, *fn_args, _meta=_meta,
            method_name=self.method_name, **fn_kwargs
        )

    async def run_worker(self, entry_id, worker):
        logger.debug(f"{self} is Spawning worker {worker}")
        worker = await self.runner.spawn_worker(self, worker)
        logger.debug(
            f"{self} Received a worker result: {worker.result}."
        )
        if worker.exception:
            can_retry = self.backoff and await self.backoff.can_retry(worker)
            if can_retry:
                worker = await self.run_backoff(entry_id, worker)

        return await self.handle_finished_worker(entry_id, worker)

    async def start_route(self, entry_id, *fn_args, _meta=None, **fn_kwargs):
        if self.stopped:
            raise ExtensionIsStopped()
        worker = await self.get_worker_instance(
            *fn_args, _meta=_meta, **fn_kwargs
        )
        return await self.run_worker(entry_id, worker)

    async def bind_to_extensions(self):
        # TODO Refactor the hole binding thing
        if self.dependencies:
            for name, dependency in self.dependencies.items():
                await dependency.bind(self.runner.config) # TODO Maybe binding later to route only

    async def stop(self):
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

    @property
    def metric_label(self):
        return self._metric_label or self.__str__().replace(" -> ", "__")

    def __str__(self):
        target = self.target
        if inspect.isfunction(target):
            target = target.__name__
        elif inspect.isclass(target):
            target = "{}.{}".format(target.__name__.lower(), self.method_name)
        elif self.method_name:
            target = "{}.{}".format(target, self.method_name)
        return f'{self.__class__.__name__} -> {target}'

    def __repr__(self):
        return self.__str__()


class CallbackRoute(Route):
    callback_worker_class = CallbackWorker

    def __init__(self, *args, callback_target,
                 callback_worker_class=None, **kwargs):
        super(CallbackRoute, self).__init__(*args, **kwargs)
        self.callback_target = callback_target
        self.callback_worker_class = callback_worker_class or self.callback_worker_class

    async def get_callback_worker_instance(self, original_worker):
        return self.callback_worker_class(
            self.callback_target, original_worker
        )

    async def start_callback_route(self, entry_id, worker):
        logger.info(f"{self.target} failed. Starting callback route.")
        callback_worker = await self.get_callback_worker_instance(worker)
        return await self.run_worker(entry_id, callback_worker)

    async def handle_finished_worker(self, entry_id, worker):
        # Callback route will only be called if retry is not valid.
        if worker is None:
            return worker

        if worker.exception and self.callback_target:
            logger.debug(
                "Finished worker has an exception, calling callback route."
            )
            worker = await self.start_callback_route(entry_id, worker)

        return super(CallbackRoute, self).handle_finished_worker(entry_id, worker)
