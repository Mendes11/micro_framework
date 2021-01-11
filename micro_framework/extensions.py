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
    context_singleton = False # Unique to the RunnerContext.
    singleton = False # Unique to the whole service.
    parent = None
    _extensions = set()


    def __new__(cls, *args, **kwargs):
        # Hack from Nameko's Extension to enable us to instantiate a new
        # Extension on Bind independent of the arguments it has on __init__
        inst = super(Extension, cls).__new__(cls)
        inst._params = (args, kwargs)

        # This could be on init, but since i'm using __new__ already...
        inst._runner = None
        inst.parent = None
        inst._extensions = set()

        return inst

    async def bind_new_extension(self, attr_name, extension, runner, parent):
        binded_ext = await extension.bind(runner, parent=parent)
        parent._extensions.add(binded_ext)
        try:
            setattr(parent, attr_name, binded_ext)
        except AttributeError:
            pass
        return binded_ext

    async def bind(self, runner, parent=None):
        """
        Binds the extension to the service runner and parent.
        :param Runner runner: Service Runner
        """
        if self.context_singleton and type(self) in runner.context_singletons:
            return runner.context_singletons[type(self)]

        if self.singleton and type(self) in runner.singletons:
            return runner.singletons[type(self)]

        if self._runner == runner:
            return self

        ext = self
        if (not self.context_singleton or self._runner != runner) and not self.singleton:
            # The context singletons are a singleton only in the same runner
            # context.
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
            extension = getattr(ext, attr_name)
            if attr_name == "parent" or attr_name == "runner":
                continue

            if extension._runner == runner:
               # Cyclic referencing
               continue

            # Bind this extension to this binded extension.
            await ext.bind_new_extension(attr_name, extension, runner, ext)
        await runner.register_extension(ext)
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
