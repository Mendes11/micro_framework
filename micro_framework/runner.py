# TODO get configurations somehow
import inspect
import logging.config
import time
from importlib import import_module

from micro_framework.extensions import Extension
from micro_framework.spawners.multiprocess import ProcessSpawner
from micro_framework.spawners.thread import ThreadSpawner

SPAWNERS = {
    'thread': ThreadSpawner,
    'process': ProcessSpawner
}

logger = logging.getLogger(__name__)


def run_function(function_path, *args, dependencies=None):
    dependencies = dependencies or {}
    *module, function = function_path.split('.')
    function = getattr(import_module('.'.join(module)), function)
    injected_dependencies = {}
    for name, dependency in dependencies.items():
        dependency.before_call()
        injected_dependencies[name] = dependency.get_dependency()

    result = function(*args, **injected_dependencies)
    for name, dependency in dependencies.items():
        dependency.after_call()
    return result


class Runner:
    def __init__(self, routes, config):
        self.config = config
        self.routes = routes
        worker_mode = config.get('WORKER_MODE', 'thread')
        self.spawner = SPAWNERS[worker_mode](config['MAX_WORKERS'])
        self.extension_spawner = ThreadSpawner()
        self.is_running = False
        self.spawned_workers = {}
        self.spawned_threads = {}
        self.extra_extensions = self._find_extensions()
        self.bind_extensions()
        logger.debug(f"Extensions: {self.extensions}")

    def _call_extensions_action(self, action, extension_set=None):
        if extension_set is None:
            extension_set = self.extensions
        for extension in extension_set:
            logger.debug(f"Calling {action} for {extension}")
            getattr(extension, action)()

    def _find_extensions(self):
        extra_extensions = set()
        for route in self.routes:
            extra_extensions.update(find_extensions(route))
        return extra_extensions

    @property
    def extensions(self):
        return {*self.routes, *self.extra_extensions}

    def bind_extensions(self):
        for extension in self.extensions:
            if not isinstance(extension, Extension):
                raise TypeError("Only Extensions should be added as a route.")
            extension.bind(self)

    def start(self):
        logger.info("Starting Runner")
        self._call_extensions_action('setup')
        self._call_extensions_action('start')
        self.is_running = True
        while self.is_running:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                self.is_running = False
                self.stop()
            except Exception:
                self.is_running = False
                self.stop()
        logger.info("Runner stopped")

    def stop(self):
        logger.info("Stopping all extensions and workers")
        # Stopping Routes First
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

    def spawn_worker(self, target_fn, callback, fn_args, dependencies=None):
        future = self.spawner.spawn(
            run_function, callback, target_fn, *fn_args,
            dependencies=dependencies
        )
        self.spawned_workers[future] = getattr(target_fn, '__name__')
        return future

    def spawn_thread(self, target_fn, callback, fn_args, fn_kwargs):
        future = self.extension_spawner.spawn(target_fn, callback, fn_args,
                                              fn_kwargs)
        self.spawned_threads[future] = getattr(target_fn, '__name__')
        return future


def find_extensions(cls):
    extensions = set()
    for name, extension in inspect.getmembers(
            cls, lambda x: isinstance(x, Extension)):
        extensions.add(extension)
        extensions.update(find_extensions(extension))
    return extensions
