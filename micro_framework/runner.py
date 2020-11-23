# TODO get configurations somehow
import asyncio
import inspect
import logging.config
import time
from concurrent.futures._base import CancelledError

from functools import partial
from timeit import default_timer


from micro_framework.config import FrameworkConfig
from micro_framework.entrypoints import Entrypoint
from micro_framework.extensions import Extension
from micro_framework.metrics import PrometheusMetricServer, tasks_counter, \
    tasks_number, tasks_failures_counter, tasks_duration, info
from micro_framework.routes import Route
from micro_framework.spawners import SPAWNERS
from micro_framework.spawners.thread import ThreadSpawner



logger = logging.getLogger(__name__)


def _spawn_worker(spawner, worker, *args, **kwargs):
    """
    This function spawns a new task using the spawner and handle the
    task metrics update.
    """
    # Increase metrics counter
    tasks_counter.inc()
    tasks_number.inc()
    start = default_timer()
    future = spawner(worker.run, *args, **kwargs)

    def on_worker_finished(future):
        worker = future.result()
        tasks_number.dec()
        if worker.exception is not None:
            tasks_failures_counter.inc()
        duration = default_timer() - start
        tasks_duration.observe(duration)

    future.add_done_callback(on_worker_finished)

    return future


class RunnerContext:
    """
    It is a context of the framework, where the routes will share the same
    spawners (and therefore the max_workers number)
    """
    # TODO Configure the MetricServer to add info about this runner instance.
    # TODO Also, we should add metrics like workers availability, so the
    #  scaler (if used) could use this metric as a parameter to know when to
    #  scale.
    def __init__(
            self, routes, worker_mode=None, max_workers=None,
            max_tasks_per_child=None
    ):
        assert isinstance(routes, list), "routes must be a list."
        assert len(routes) > 0, "routes must have a length higher than zero."

        self.routes = routes
        self.worker_mode = worker_mode
        self.max_workers = max_workers
        self.max_tasks_per_child = max_tasks_per_child

        self.is_running = False
        self.spawned_extensions = {}
        self.entrypoints = set()
        self.extra_extensions = set()

        self.bind_extensions()
        self._find_extensions()

        logger.debug(f"Extensions: {self.extensions}")

    async def bind(self, runner):
        """
        Bind to a runner instance.
        :param runner:
        :return:
        """
        self.runner = runner

    async def setup(self):
        self.config = self.runner.config
        self.metric_server = self.runner.metric_server

        if self.max_workers is None:
            self.max_workers = self.config["MAX_WORKERS"]
        if self.worker_mode is None:
            self.worker_mode = self.config["WORKER_MODE"]
        if self.max_tasks_per_child is None:
            self.max_tasks_per_child = self.config["MAX_TASKS_PER_CHILD"]

        self.spawner = self.runner.get_spawner(
            self.worker_mode, max_workers=self.max_workers,
            max_tasks_per_child=self.max_tasks_per_child
        )

        info.info(
            {
                'max_workers': str(self.max_workers),
                'worker_mode': self.worker_mode,
                'max_tasks_per_child': str(self.max_tasks_per_child)
            }
        )

    def _call_extensions_action(self, action, extension_set=None):
        if extension_set is None:
            extension_set = self.extensions
        for extension in extension_set:
            logger.debug(f"Calling {action} for {extension}")
            getattr(extension, action)()

    def _find_extensions(self):
        for route in self.routes:
            extensions = find_extensions(route)
            for extension in extensions:
                self.register_extension(extension)

    @property
    def extensions(self):
        return {*self.routes, *self.entrypoints, *self.extra_extensions}

    def bind_extensions(self):
        for route in self.routes:
            if not isinstance(route, Extension):
                raise TypeError("Only Extensions should be added as a "
                                "entrypoints.")
            route.bind(self)

    def register_extension(self, extension):
        if isinstance(extension, Entrypoint):
            self.entrypoints.add(extension)
        else:
            self.extra_extensions.add(extension)

    async def start(self):
        self.event_loop = asyncio.get_event_loop()
        logger.info(
            f"Starting Runner with {self.max_workers}"
            f" {self.worker_mode} workers"
        )

        # First we tell the routes to bind to their extensions
        self._call_extensions_action(
            'bind_to_extensions', extension_set=self.routes
        )
        logger.debug(f"Extensions: {self.extensions}")

        # Once we have all extensions detected:
        # Setup Routes first then entrypoints and then extra extensions.
        self._call_extensions_action('setup', extension_set=self.routes)
        self._call_extensions_action('setup', extension_set=self.entrypoints)
        self._call_extensions_action('setup',
                                     extension_set=self.extra_extensions)

        # Finally, we start all extensions.
        self._call_extensions_action('start')

    async def stop(self):
        self.is_running = False

        logger.info("Stopping all context extensions and workers")

        # Stopping Entrypoints First
        self._call_extensions_action('stop', extension_set=self.routes)

        # Stop Workers
        logger.debug("Stopping workers")
        self.spawner.stop(wait=True)

        logger.debug("Stopping extra extensions")
        self._call_extensions_action('stop',
                                     extension_set=self.extra_extensions)

        # Stop running extensions
        logger.debug("Stopping any still running extensions thread")
        # self.extension_spawner.stop(wait=False)
        # Summary: Please Stop!!
        for task in self.spawned_extensions:
            task.cancel()

        # self.event_loop.call_soon_threadsafe(self.event_loop.stop)
        # self.event_loop.stop()
        # self.event_loop.close()

    def spawn_worker(self, worker, *fn_args, callback=None, **fn_kwargs):
        """
        Spawn a new worker to execute a function from the calling route.

        :param Route route: The route that called it
        :param fn_args: Arguments of the function to be called.
        :return Future: A Future object of the task.
        """
        future = _spawn_worker(
            self.spawner.spawn, worker, callback, *fn_args, **fn_kwargs
        )
        return future

    def spawn_async_extension(self, extension, coroutine, callback=None):
        if callback is None:
            callback = self.extension_thread_callback

        task = asyncio.ensure_future(coroutine)
        task.add_done_callback(callback)
        self.spawned_extensions[task] = extension
        return task

    def spawn_extension(self, extension, target_fn, *fn_args, callback=None,
                        **fn_kwargs):
        """
        Any extension that needs an async call should call this method to
        start it.

        :param Extension extension: An extension subclass
        :param function target_fn: the function to be executed.
        :param function callback: A callback function to handle the
        result/error
        :param fn_args: Any argument to be passed to the target function.
        :param fn_kwargs: Any named argument to be passed to the target
        function.
        :return Future: A future object of the task.
        """
        if inspect.iscoroutinefunction(target_fn):
            return self.spawn_async_extension(
                extension, target_fn(*fn_args, **fn_kwargs), callback
            )

        if callback is None:
            callback = self.extension_thread_callback

        target_fn = partial(target_fn, *fn_args, **fn_kwargs)
        task = self.event_loop.run_in_executor(None, target_fn)

        task.add_done_callback(callback)

        self.spawned_extensions[task] = extension
        return task

    def extension_thread_callback(self, future):
        """
        Handle the result of an extension when it's worker is finished either
        by an error or simply shutting down.
        """
        if not future.cancelled() and future.exception() and not isinstance(
                future.exception(), (CancelledError, asyncio.CancelledError)):
            raise future.exception()
            # logger.error(f"Exception in thread: \n{future.exception()}")
            # self.stop()
            # raise future.exception()


class Runner:
    def __init__(self, contexts, config, metric_server=None):
        assert isinstance(contexts, list), "contexts must be a list"
        assert len(contexts) > 0, "contexts list must have at least one " \
                                  "RunnerContext"

        self.spawners = []
        self.config = FrameworkConfig(config)
        if isinstance(contexts[0], Route):
            contexts = [RunnerContext(contexts)]
        self.contexts = contexts

        # Metric Extension Binding
        if metric_server is None:
            metric_server = PrometheusMetricServer()
        self.metric_server = metric_server
        self.metric_server.bind(self)

    def get_spawner(self, spawner_type, **spawner_configs):
        """
        Returns and register a new spawner instance.
        :param str spawner_type: The type of the Spawner
        :param spawner_configs: That spawner configurations
        :return Spawner: A spawner instance.
        """
        spawner = SPAWNERS[spawner_type](**spawner_configs)
        self.spawners.append(spawner)
        return spawner

    async def _start(self):
        self.event_loop = asyncio.get_event_loop()
        for context in self.contexts:
            await context.bind(self)
            await context.setup()
        self.metric_server.setup()

        self.context_tasks = [
            self.event_loop.create_task(context.start())
            for context in self.contexts
        ]

        self.event_loop.run_in_executor(None, self.metric_server.start)

    def start(self):
        self.event_loop = asyncio.get_event_loop()
        self.event_loop.create_task(self._start())
        try:
            self.event_loop.run_forever()
        except (KeyboardInterrupt, asyncio.CancelledError):
            self.stop()
        except Exception:
            self.stop()
            raise

    def stop(self):

        # Stopping all contexts
        self.event_loop.run_until_complete(
            asyncio.gather(*[context.stop() for context in self.contexts])
        )
        self.metric_server.stop()

        self.event_loop.close()
        logger.info("Runner stopped")


def find_extensions(cls):
    extensions = set()
    for name, extension in inspect.getmembers(
            cls, lambda x: isinstance(x, Extension)):
        extensions.add(extension)
        extensions.update(find_extensions(extension))
    return extensions
