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
            self, routes=None, worker_mode=None, max_workers=None,
            context_metric_label=None
    ):
        if routes is None:
            routes = []

        self.routes = []
        self.config = {}
        if max_workers:
            self.config["MAX_WORKERS"] = max_workers

        if worker_mode:
            self.config["WORKER_MODE"] = worker_mode

        self.is_running = False
        self.spawned_extensions = {}
        self.entrypoints = set()
        self.extra_extensions = set()
        self.context_singletons = {} # {type: instance}
        self.metric_label = context_metric_label or f"context_{id(self)}"
        self.running_routes = 0
        self.add_routes(*routes)

    def add_routes(self, *routes):
        self.routes += routes
        return self

    async def bind(self, runner):
        self.runner = runner

    async def setup(self):
        # Update this context config with the runner config to fill missing
        # configurations.
        self.config = FrameworkConfig(
            self.config, default_config=self.runner.config
        )
        self.metric_server = self.runner.metric_server

        await self.bind_routes()
        logger.debug(f"Extensions: {self.extensions}")

        info.labels(self.metric_label).info(
            {
                'max_workers': str(self.config["MAX_WORKERS"]),
                'worker_mode': self.config["WORKER_MODE"],
            }
        )
        available_workers.labels(self.metric_label).set(self.available_workers)

    async def _call_extensions_action(self, action, extension_set=None):
        if extension_set is None:
            extension_set = self.extensions
        tasks = [getattr(extension, action)() for extension in extension_set]
        await asyncio.gather(*tasks)

    async def _find_extensions(self):
        for route in self.routes:
            extensions = await find_extensions(route)
            for extension in extensions:
                await self.register_extension(extension)

    @property
    def extensions(self):
        return {*self.routes, *self.entrypoints, *self.extra_extensions}

    async def bind_routes(self):
        binded_routes = []
        for route in self.routes:
            if not isinstance(route, Route):
                raise TypeError("Only Route class should be added as a "
                                "route.")
            binded_routes.append(route.bind(self))
        self.routes = binded_routes

    def register_extension(self, extension):
        if isinstance(extension, Entrypoint) and not extension.singleton:
            self.entrypoints.add(extension)
        else:
            self.extra_extensions.add(extension)

        if extension.context_singleton:
            existing = self.context_singletons.get(type(extension))
            if existing is not None and existing != extension:
                raise TypeError(
                    "Extension {} is a context singleton and already has a "
                    "registered Extension: {}".format(
                        extension, existing
                    )
                )
            self.context_singletons[type(extension)] = extension
        if extension.singleton:
           self.runner.register_extension(extension)

    async def start(self):
        self.event_loop = asyncio.get_event_loop()
        logger.info(
            f"Starting Runner with {self.config['MAX_WORKERS']}"
            f" {self.config['WORKER_MODE']} workers"
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
        self.spawner = await self.runner.get_spawner(self)

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
            self, route, worker, callback=None
    ):
        """
        Spawn a new worker to execute a function from the calling route.

        :param Route route: The route that called it
        :param fn_args: Arguments of the function to be called.
        :return Future: A Future object of the task.
        """
        if not self.is_running:
            raise ExtensionIsStopped()

        # Update metrics before starting the worker.
        tasks_counter.labels(self.metric_label, route.metric_label).inc()
        tasks_number.labels(self.metric_label, route.metric_label).inc()
        available_workers.labels(self.metric_label).dec()
        start = default_timer()

        # Run the worker until it completes the task

        # Note that since we are working with asyncio, we don't need to
        # create a task, the await will automatically giveup on the lock and
        # only return when the task in the worker is done.
        self.running_routes += 1
        await worker.run(self)
        self.running_routes -= 1

        # Update the metrics after the worker is finished.
        tasks_number.labels(self.metric_label, route.metric_label).dec()
        available_workers.labels(self.metric_label).inc()
        if worker.exception is not None:
            tasks_failures_counter.labels(
                self.metric_label, route.metric_label
            ).inc()
        duration = default_timer() - start
        tasks_duration.labels(
            self.metric_label, route.metric_label
        ).observe(duration)

        return worker

    def execute_async(self, coroutine, raise_for_future=True):
        """
        Executes a coroutine in the background, without awaiting it.

        This method is to normal functions/methods from extensions to call an
        async function.

        :param coroutine: Coroutine
        :return Future: Future object.
        """
        future = asyncio.run_coroutine_threadsafe(coroutine, self.event_loop)
        def on_finished_task(future):
            if future.exception() and raise_for_future:
                raise future.exception()
        future.add_done_callback(on_finished_task)
        return future

    async def spawn_extension(
            self, extension, target_fn, *fn_args, **fn_kwargs
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
        """
        if inspect.iscoroutinefunction(target_fn):
            coroutine = target_fn(*fn_args, **fn_kwargs)
            try:
                await coroutine
            except (CancelledError, asyncio.CancelledError):
                return

        else:  # Sync Functions will run in the default executor
            target_fn = partial(target_fn, *fn_args, **fn_kwargs)
            future = self.event_loop.run_in_executor(None, target_fn)

            future.add_done_callback(self.extension_thread_callback)
            self.spawned_extensions[future] = extension

    def extension_thread_callback(self, future):
        """
        Handle the result of an extension when it's worker is finished either
        by an error or simply shutting down.
        """
        if not future.cancelled() and future.exception() and not isinstance(
                future.exception(), (CancelledError, asyncio.CancelledError)):
            raise future.exception()

    @property
    def max_workers(self):
        return self.config["MAX_WORKERS"]

    @property
    def available_workers(self):
        return self.max_workers - self.running_routes

    @property
    def singletons(self):
        return self.runner.singletons


class Runner:
    def __init__(self, contexts=None, config=None, metric_server=None):
        if contexts is None:
            contexts = []

        if config is None:
            config = {}

        self.spawners = []
        self.config = FrameworkConfig(config)
        if contexts and isinstance(contexts[0], Route):
            contexts = [RunnerContext(contexts)]
        self.contexts = contexts
        self.metric_server = metric_server
        self.extensions = set()
        self.singletons = {}

    def exception_handler(self, loop, context):
        asyncio.run_coroutine_threadsafe(self._stop(), self.event_loop)
        if context.get("exception"):
            raise context.get("exception")

    def add_contexts(self, *contexts):
        self.contexts += contexts

    def new_context(
            self, routes=None, worker_mode=None, max_workers=None,
            context_metric_label=None, inplace=False):
        ctx = RunnerContext(routes, worker_mode, max_workers, context_metric_label)
        if inplace:
            self.add_contexts(ctx)
        return ctx

    def register_extension(self, extension):
        if extension.singleton:
            existing = self.singletons.get(type(extension))
            if existing is not None and existing != extension:
                raise TypeError(
                    "Extension {} is a singleton and already has a registered "
                    "Extension: {}".format(extension, existing)
                )
            self.singletons[type(extension)] = extension
        self.extensions.add(extension)

    async def get_spawner(self, runner_context: RunnerContext):
        """
        Returns and register a new spawner instance.
        :param str spawner_type: The type of the Spawner
        :param spawner_configs: That spawner configurations
        :return Spawner: A spawner instance.
        """
        spawner = SPAWNERS[runner_context.config["WORKER_MODE"]](
            runner_context.config
        )
        self.spawners.append(spawner)
        return spawner

    async def _start(self):
        # Metric Extension Binding
        self.event_loop = asyncio.get_event_loop()

        if self.metric_server is None:
            self.metric_server = PrometheusMetricServer()
        self.metric_server = self.metric_server.bind(self)

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


async def find_extensions(cls: Extension, existing_extensions=None):
    """
    Searches for extensions inside a given class.


    :param cls: Any class
    :param set existing_extensions: A set of extensions already registered.
    :return set: Set of extensions found
    """
    if existing_extensions is None:
        existing_extensions = set()
    extensions = existing_extensions

    for extension in cls._extensions:
        if extension in existing_extensions:
            continue
        extensions.add(extension)
        extensions.update(await find_extensions(extension, extensions))
    return extensions
