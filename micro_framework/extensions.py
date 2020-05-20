import inspect
import logging

logger = logging.getLogger(__name__)


class Extension:
    """
    An element to be bind into the Service Runner.
    It also binds sub-extensions (Extension inside another)
    """
    runner = None

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
