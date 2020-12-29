# TODO get configurations somehow
import asyncio
import inspect
import logging.config
import sys
import time
from concurrent.futures._base import CancelledError

from functools import partial
from timeit import default_timer


from micro_framework.config import FrameworkConfig
from micro_framework.entrypoints import Entrypoint
from micro_framework.exceptions import ExtensionIsStopped
from micro_framework.extensions import Extension
from micro_framework.metrics import PrometheusMetricServer, tasks_counter, \
    tasks_number, tasks_failures_counter, tasks_duration, info, \
    available_workers
from micro_framework.routes import Route
from micro_framework.spawners import SPAWNERS
from micro_framework.spawners.thread import ThreadSpawner



logger = logging.getLogger(__name__)


class RunnerContext:
    """
    It is a context of the framework, where the routes will share the same
    spawners (and therefore the max_workers number)
    """
    def __init__(
            self, routes, worker_mode=None, max_workers=None,
            max_tasks_per_child=None, context_metric_label=None
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
        self.metric_label = context_metric_label or f"context_{id(self)}"
        self.running_routes = {}

    async def bind(self, runner):
        """
        Bind to a runner instance.
        :param runner:
        :return:
        """
        self.runner = runner

    async def setup(self):
        await self.bind_routes()
        await self._find_extensions()
        logger.debug(f"Extensions: {self.extensions}")

        self.config = self.runner.config
        self.metric_server = self.runner.metric_server

        if self.max_workers is None:
            self.max_workers = self.config["MAX_WORKERS"]
        if self.worker_mode is None:
            self.worker_mode = self.config["WORKER_MODE"]
        if self.max_tasks_per_child is None:
            self.max_tasks_per_child = self.config["MAX_TASKS_PER_CHILD"]

        self.spawner = await self.runner.get_spawner(
            self.worker_mode, max_workers=self.max_workers,
            max_tasks_per_child=self.max_tasks_per_child
        )

        info.labels(self.metric_label).info(
            {
                'max_workers': str(self.max_workers),
                'worker_mode': self.worker_mode,
                'max_tasks_per_child': str(self.max_tasks_per_child)
            }
        )
        available_workers.labels(self.metric_label).set(self.available_workers)

    async def _call_extensions_action(self, action, extension_set=None):
        if extension_set is None:
            extension_set = self.extensions
        for extension in extension_set:
            logger.debug(f"Calling {action} for {extension}")
            await getattr(extension, action)()

    async def _find_extensions(self):
        for route in self.routes:
            extensions = await find_extensions(route)
            for extension in extensions:
                await self.register_extension(extension)

    @property
    def extensions(self):
        return {*self.routes, *self.entrypoints, *self.extra_extensions}

    async def bind_routes(self):
        for route in self.routes:
            if not isinstance(route, Route):
                raise TypeError("Only Route class should be added as a "
                                "route.")
            await route.bind(self)

    async def register_extension(self, extension):
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
        await self._call_extensions_action(
            'bind_to_extensions', extension_set=self.routes
        )
        logger.debug(f"Extensions: {self.extensions}")

        # Once we have all extensions detected:
        # Setup Routes first then entrypoints and then extra extensions.
        await self._call_extensions_action('setup', extension_set=self.routes)
        await self._call_extensions_action(
            'setup', extension_set=self.entrypoints
        )
        await self._call_extensions_action(
            'setup', extension_set=self.extra_extensions
        )

        # Finally, we start all extensions.
        self.is_running = True
        await self._call_extensions_action('start')

    async def stop(self):
        self.is_running = False

        logger.info("Stopping all context extensions and workers")

        # Stopping Routes
        await self._call_extensions_action('stop', extension_set=self.routes)

        # Stop Workers
        logger.debug("Stopping workers")
        self.spawner.stop(wait=True)

        logger.debug("Stopping extra extensions")
        await self._call_extensions_action(
            'stop', extension_set=self.extra_extensions
        )

        # Stop running extensions
        logger.debug("Stopping any still running extensions thread")
        # self.extension_spawner.stop(wait=False)
        # Summary: Please Stop!!
        for task in self.spawned_extensions:
            task.cancel()

    async def spawn_worker(
            self, route, worker, *fn_args, callback=None, **fn_kwargs
    ):
        """
        Spawn a new worker to execute a function from the calling route.

        :param Route route: The route that called it
        :param fn_args: Arguments of the function to be called.
        :return Future: A Future object of the task.
        """
        if not self.is_running:
            raise ExtensionIsStopped()

        tasks_counter.labels(self.metric_label, route.metric_label).inc()
        tasks_number.labels(self.metric_label, route.metric_label).inc()
        available_workers.labels(self.metric_label).dec()
        start = default_timer()
        future = self.spawner.spawn(worker.run, *fn_args, **fn_kwargs)
        self.running_routes[future] = route

        def update_worker_metrics(future):
            worker = future.result()
            tasks_number.labels(self.metric_label, route.metric_label).dec()
            available_workers.labels(self.metric_label).inc()
            self.running_routes.pop(future, None)
            if worker.exception is not None:
                tasks_failures_counter.labels(
                    self.metric_label, route.metric_label
                ).inc()
            duration = default_timer() - start
            tasks_duration.labels(
                self.metric_label, route.metric_label
            ).observe(duration)

            if callback:
                self.execute_async(callback(future))

        future.add_done_callback(update_worker_metrics)
        return future

    def execute_async(self, coroutine):
        """
        Executes a coroutine in the background, without awaiting it.

        This method is to normal functions/methods from extensions to call an
        async function.

        :param coroutine: Coroutine
        :return Future: Future object.
        """
        future = asyncio.run_coroutine_threadsafe(coroutine, self.event_loop)
        def on_finished_task(future):
            if future.exception():
                raise future.exception()
        future.add_done_callback(on_finished_task)
        return future

    async def spawn_extension(
            self, extension, target_fn, *fn_args, callback=None, **fn_kwargs
    ):
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
            coroutine = target_fn(*fn_args, **fn_kwargs)
            future = self.execute_async(coroutine)
        else:
            target_fn = partial(target_fn, *fn_args, **fn_kwargs)
            future = self.event_loop.run_in_executor(None, target_fn)

        if callback is None:
            callback = self.extension_thread_callback

        future.add_done_callback(callback)
        self.spawned_extensions[future] = extension
        return future

    def extension_thread_callback(self, future):
        """
        Handle the result of an extension when it's worker is finished either
        by an error or simply shutting down.
        """
        if not future.cancelled() and future.exception() and not isinstance(
                future.exception(), (CancelledError, asyncio.CancelledError)):
            raise future.exception()

    @property
    def available_workers(self):
        return self.max_workers - len(self.running_routes)


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
        self.metric_server = metric_server

    def exception_handler(self, loop, context):
        asyncio.run_coroutine_threadsafe(self._stop(), self.event_loop)
        if context.get("exception"):
            raise context.get("exception")

    async def get_spawner(self, spawner_type, **spawner_configs):
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
        # Metric Extension Binding
        self.event_loop = asyncio.get_event_loop()

        if self.metric_server is None:
            self.metric_server = PrometheusMetricServer()
        await self.metric_server.bind(self)

        for context in self.contexts:
            await context.bind(self)
            await context.setup()
        await self.metric_server.setup()
        await self.metric_server.start()


        for context in self.contexts:
            await context.start()

    async def _stop(self):
        # TODO Ensure that all spawner pools are stopped so then we stop the
        #  rest
        cancel_contexts = [context.stop() for context in self.contexts]
        await asyncio.gather(*cancel_contexts, return_exceptions=True)

        if sys.version_info[1] < 7:
            tasks = [
                t for t in asyncio.Task.all_tasks(self.event_loop)
                if t is not asyncio.Task.current_task(self.event_loop)
            ]
        else:
            tasks = [
                t for t in asyncio.all_tasks() if t is not asyncio.current_task()
            ]
        for task in tasks: task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.metric_server.stop()
        self.event_loop.stop()
        logger.info("Runner stopped")


    def start(self):
        self.event_loop = asyncio.get_event_loop()
        self.event_loop.set_exception_handler(self.exception_handler)
        try:
            self.event_loop.run_until_complete(self._start())
            self.event_loop.run_forever()
        except (KeyboardInterrupt, asyncio.CancelledError):
            self.event_loop.run_until_complete(self._stop())
        except Exception:
            self.event_loop.run_until_complete(self._stop())
            raise


async def find_extensions(cls):
    extensions = set()
    for name, extension in inspect.getmembers(
            cls, lambda x: isinstance(x, Extension)):
        extensions.add(extension)
        extensions.update(await find_extensions(extension))
    return extensions
