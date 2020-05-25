import inspect
import logging

logger = logging.getLogger(__name__)


class Extension:
    """
    An element to be bind into the Service Runner.
    It also binds sub-extensions (Extension inside another)
    """
    _runner = None
    _params = None

    def __new__(cls, *args, **kwargs):
        # Hack from Nameko's Extension to enable us to instantiate a new
        # Extension on Bind independent of the arguments it has on __init__
        inst = super(Extension, cls).__new__(cls)
        inst._params = (args, kwargs)
        return inst

    def bind(self, runner):
        """
        Binds the extension to the service runner and parent.
        :param Runner runner: Service Runner
        """
        if self._runner is not None:
            return self._runner
            # TODO Letting all extensions being shareable will impact in
            #  something?
            #raise AssertionError("This extension is already bound.")
        self._runner = runner
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

    def _clone(self):
        cls = type(self)
        return cls(*self._params[0], **self._params[1])

    @property
    def runner(self):
        if self._runner is None:
            raise RuntimeError("This extension is not binded. You must bind "
                               "it to a runner before trying to attempt this.")
        return self._runner
