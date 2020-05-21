import inspect
import logging

logger = logging.getLogger(__name__)


class Extension:
    """
    An element to be bind into the Service Runner.
    It also binds sub-extensions (Extension inside another)
    """
    runner = None
    __params = None

    def __new__(cls, *args, **kwargs):
        # Hack from Nameko's Extension to enable us to instantiate a new
        # Extension on Bind independent of the arguments it has on __init__
        inst = super(Extension, cls).__new__(cls)
        inst.__params = (args, kwargs)
        return inst

    def bind(self, runner):
        """
        Binds the extension to the service runner.
        :param Runner runner: Service Runner
        """
        if self.runner is not None:
            raise AssertionError("This extension is already bound.")
        self.runner = runner
        # Binding sub-extensions
        for attr_name, extension in inspect.getmembers(
                self, lambda x: isinstance(x, Extension)):
            extension.bind(runner)

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
