# TODO get configurations somehow
import asyncio
import inspect
import logging.config
from timeit import default_timer

from aiohttp.web_runner import GracefulExit

from micro_framework.config import FrameworkConfig
from micro_framework.entrypoints import Entrypoint
from micro_framework.extensions import Extension
from micro_framework.metrics import PrometheusMetricServer, tasks_counter, \
    tasks_number, tasks_failures_counter, tasks_duration, info
from micro_framework.spawners.multiprocess import ProcessSpawner, \
    ProcessPoolSpawner
from micro_framework.spawners.thread import ThreadSpawner, ThreadPoolSpawner

# Greedy Spawner means that the runner will consume all received tasks so far
# when a stop signal is sent.

# The non-greedy spawner will finish the current tasks and skip all pending
# ones. (Entrypoints with acknowledge are good candidates for this)
SPAWNERS = {
    'thread': ThreadSpawner,  # Will
    'process': ProcessSpawner,
    'greedy_thread': ThreadPoolSpawner,
    # Will consume all received tasks on stop
    'greedy_process': ProcessPoolSpawner,  # Will consume all received tasks
}

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


class Runner:
    def __init__(self, routes, config, metric_server=None):
        self.config = FrameworkConfig(config)
        self.routes = routes
        self.worker_mode = self.config['WORKER_MODE']
        if self.worker_mode == 'process':
            self.spawner = SPAWNERS[self.worker_mode](
                self.config['MAX_WORKERS'], self.config['MAX_TASKS_PER_CHILD']
            )
        else:
            self.spawner = SPAWNERS[self.worker_mode](
                self.config['MAX_WORKERS']
            )
        self.extension_spawner = ThreadSpawner()
        self.is_running = False
        self.spawned_workers = {}
        self.spawned_threads = {}
        self.bind_extensions()
        self.entrypoints = set()
        self.extra_extensions = set()
        self._find_extensions()

        # Metric Extension Binding
        if metric_server is None:
            metric_server = PrometheusMetricServer()
        self.metric_server = metric_server
        self.metric_server.bind(self)
        self.register_extension(self.metric_server)
        logger.debug(f"Extensions: {self.extensions}")
        info.info(
            {
                'max_workers': str(self.config['MAX_WORKERS']),
                'worker_mode': self.config['WORKER_MODE'],
                'max_tasks_per_child': str(self.config['MAX_TASKS_PER_CHILD'])
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

    def start(self):
        self.event_loop = asyncio.get_event_loop()
        logger.info(
            f"Starting Runner with {self.config['MAX_WORKERS']}"
            f" {self.worker_mode} workers"
        )
        # First we tell the routes to bind to their extensions
        self._call_extensions_action(
            'bind_to_extensions', extension_set=self.routes
        )
        logger.debug(f"Extensions: {self.extensions}")
        # Once we have all extensions detected:
        # Setup Routes first then entrypoints last and extra extensions last.
        self._call_extensions_action('setup', extension_set=self.routes)
        self._call_extensions_action('setup', extension_set=self.entrypoints)
        self._call_extensions_action('setup',
                                     extension_set=self.extra_extensions)

        self._call_extensions_action('start')
        try:
            self.event_loop.run_forever()
        except (KeyboardInterrupt, GracefulExit, asyncio.CancelledError):
            self.stop()
        except Exception:
            self.stop()
            raise
        logger.info("Runner stopped")

    def stop(self, *args):
        self.is_running = False

        logger.info("Stopping all extensions and workers")

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
        self.extension_spawner.stop(wait=False)

        # Summary: Please Stop!!
        for task in asyncio.Task.all_tasks():
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
            self.spawner.spawn, worker, callback, *fn_args, **fn_kwargs)
        self.spawned_workers[future] = worker
        return future

    def spawn_extension(self, extension, target_fn, *fn_args, callback=None,
                        **fn_kwargs):
        """
        Any extension that needs a separate thread should call this method to
        start a new thread for it.
        :param Extension extension: An extension subclass
        :param function target_fn: the function to be executed.
        :param function callback: A callback function to handle the
        result/error
        :param fn_args: Any argument to be passed to the target function.
        :param fn_kwargs: Any named argument to be passed to the target
        function.
        :return Future: A future object of the task.
        """
        if callback is None:
            callback = self.extension_thread_callback
        future = self.extension_spawner.spawn(
            target_fn, callback, *fn_args, **fn_kwargs
        )
        self.spawned_threads[future] = extension
        return future

    def extension_thread_callback(self, future):
        """
        Handle the result of an extension when it's worker is finished either
        by an error or simply shutting down.
        """
        if future.exception():
            raise future.exception()
            # logger.error(f"Exception in thread: \n{future.exception()}")
            # self.stop()
            # raise future.exception()


def find_extensions(cls):
    extensions = set()
    for name, extension in inspect.getmembers(
            cls, lambda x: isinstance(x, Extension)):
        extensions.add(extension)
        extensions.update(find_extensions(extension))
    return extensions
