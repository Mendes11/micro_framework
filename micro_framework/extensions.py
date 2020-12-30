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
    singleton = False
    parent = None

    def __new__(cls, *args, **kwargs):
        # Hack from Nameko's Extension to enable us to instantiate a new
        # Extension on Bind independent of the arguments it has on __init__
        inst = super(Extension, cls).__new__(cls)
        inst._params = (args, kwargs)
        return inst

    async def bind(self, runner, parent=None):
        """
        Binds the extension to the service runner and parent.
        :param Runner runner: Service Runner
        """
        if self._runner is not None:
            return self


        ext = self
        if not self.singleton:
            # We copy this extension to prevent some threading problems
            ext = self._clone()
            ext.parent = parent

        else:
            ext.parent = runner

        ext._runner = runner

        # Retrieve the extensions declared inside this extension
        inner_extensions = inspect.getmembers(
            ext, lambda x: isinstance(x, Extension) and x._runner is None
        )
        for attr_name, extension in inner_extensions:
            if attr_name == "parent" or attr_name == "runner":
                continue

            if extension._runner is not None and extension.parent is not None:
               # Cyclic referencing
               continue

            binded_ext = await extension.bind(runner, parent=ext)
            try:
                setattr(ext, attr_name, binded_ext)
            except AttributeError:
                continue

        return ext

    async def setup(self):
        """
        Called After the binding and before the system start
        """
        pass

    async def start(self):
        """
        Commands the extension to start running.
        """
        pass

    async def stop(self):
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
