# TODO get configurations somehow
import inspect
import logging.config
import time

from micro_framework.entrypoints import Entrypoint
from micro_framework.extensions import Extension
from micro_framework.spawners.multiprocess import ProcessSpawner, \
    ProcessPoolSpawner
from micro_framework.spawners.thread import ThreadSpawner, ThreadPoolSpawner


# Greedy Spawner means that the runner will consume all received tasks so far
# when a stop signal is sent.

# The non-greedy spawner will finish the current tasks and skip all pending
# ones. (Entrypoints with acknowledge are good candidates for this)
SPAWNERS = {
    'thread': ThreadSpawner, # Will
    'process': ProcessSpawner,
    'greedy_thread': ThreadPoolSpawner, # Will consume all received tasks on stop
    'greedy_process': ProcessPoolSpawner, # Will consume all received tasks
}

logger = logging.getLogger(__name__)


class Runner:
    def __init__(self, routes, config):
        self.config = config
        self.routes = routes
        self.worker_mode = config.get('WORKER_MODE', 'thread')
        if self.worker_mode == 'process':
            self.spawner = SPAWNERS[self.worker_mode](
                config['MAX_WORKERS'], config['MAX_TASKS_PER_CHILD']
            )
        else:
            self.spawner = SPAWNERS[self.worker_mode](
                config['MAX_WORKERS']
            )
        self.extension_spawner = ThreadSpawner()
        self.is_running = False
        self.spawned_workers = {}
        self.spawned_threads = {}
        self.bind_extensions()
        self.entrypoints = set()
        self.extra_extensions = set()
        self._find_extensions()
        logger.debug(f"Extensions: {self.extensions}")

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
        self._call_extensions_action('setup', extension_set=self.extra_extensions)

        self._call_extensions_action('start')
        self.is_running = True
        while self.is_running:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
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

    def spawn_worker(self, worker, *fn_args, callback=None, **fn_kwargs):
        """
        Spawn a new worker to execute a function from the calling route.

        :param Route route: The route that called it
        :param fn_args: Arguments of the function to be called.
        :return Future: A Future object of the task.
        """
        future = self.spawner.spawn(
            worker.run, callback, *fn_args, **fn_kwargs
        )
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
            print("Exception in thread.")
            self.stop()
            raise future.exception()


def find_extensions(cls):
    extensions = set()
    for name, extension in inspect.getmembers(
            cls, lambda x: isinstance(x, Extension)):
        extensions.add(extension)
        extensions.update(find_extensions(extension))
    return extensions
