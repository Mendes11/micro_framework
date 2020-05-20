# TODO get configurations somehow
import inspect
import logging.config
import signal
import time
from functools import partial
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


class Worker:
    def __init__(self, route, failure_route=False):
        self.function_path = route.function_path
        if failure_route:
            self.function_path = route.exception_function_path
        self.config = route.runner.config
        self._dependencies = route.dependencies
        self._translators = route.translators

    def call_dependencies(self, action, *args, **kwargs):
        ret = {}
        for name, dependency in self._dependencies.items():
            ret[name] = getattr(dependency, action)(*args, **kwargs)
        return ret

    def get_callable(self):
        *module, function = self.function_path.split('.')
        function = getattr(import_module('.'.join(module)), function)
        return function

    def start(self, *args):
        self.function = self.get_callable()
        self.call_dependencies('setup')
        self.dependencies = self.call_dependencies('get_dependency', self)
        self.call_dependencies('before_call', self)
        result = None
        fn_args = args
        for translator in self._translators:
            fn_args = translator.translate(*fn_args)
        try:
            result = self.function(*args, **self.dependencies)
        except Exception as exc:
            self.call_dependencies('after_call', result, exc, self)
            raise
        self.call_dependencies('after_call', result, None, self)
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
                raise
        logger.info("Runner stopped")

    def stop(self, *args):
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

    def spawn_worker(self, route, callback, fn_args):
        """
        Spawn a new worker to execute a function from the calling route.

        :param Route route: The route that called it
        :param function callback: A callback to handle the result
        :param fn_args: Arguments of the function to be called.
        :return Future: A Future object of the task.
        """
        worker = Worker(route)
        future = self.spawner.spawn(
            worker.start, callback, *fn_args
        )
        self.spawned_workers[future] = route
        return future

    def spawn_extension(self, extension, target_fn, callback, *fn_args,
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


def find_extensions(cls):
    extensions = set()
    for name, extension in inspect.getmembers(
            cls, lambda x: isinstance(x, Extension)):
        extensions.add(extension)
        extensions.update(find_extensions(extension))
    return extensions
