import inspect
import logging

logger = logging.getLogger(__name__)


class Extension:
    """
    An element to be bind into the Service Runner.
    It also binds sub-extensions (Extension inside another)
    """

    def bind(self, runner):
        """
        Binds the extension to the service runner.
        :param Runner runner: Service Runner
        """
        if not self.runner:
            self.runner = runner
        for attr_name, atrr_value in inspect.getmembers(
                self, lambda x: isinstance(x, Extension)):
            atrr_value.bind(runner)

    def setup(self):
        """
        Called After the binding and before the system start
        """
        pass

    def start(self):
        """
        Commands the extension to start running.
        """
        pass

    def stop(self):
        """
        Commands the extension to do a graceful stop
        """
        pass


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

    def __init__(self, function_path, dependencies=None):
        self.dependencies = dependencies
        self.function_path = function_path
        self.runner = None

    def call_function(self, *args, callback=None):
        future = self.runner.spawn_worker(
            target_fn=self.function_path,
            callback=callback,
            fn_args=args,
            dependencies=self.dependencies
        )
        if not callback:
            return future.result(timeout=None)
        return future

    def __str__(self):
        return f'{self.__class__.__name__} -> {self.function_path}'

    def __repr__(self):
        return self.__str__()


class Dependency(Extension):
    """
    Kind of extension that injects a dependency into the function.
    """

    def before_call(self):
        """
        Treatment before the actual function call.
        """
        pass

    def get_dependency(self):
        """
        Injects the dependency.
        """
        pass

    def after_call(self):
        """
        Cleanup after the function has finished.
        """
        pass
