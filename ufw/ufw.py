import importlib
import pkgutil
from functools import wraps

class Node:
    """
    A unit of work, with methods to edit the topology using decorators
    """
    def __init__(self, app, callable):
        self.callable = callable
        self.app = app

    def then(self, new_callable):
        return self.app.new_node(self, new_callable)

    def __call__(self, *args, **kwargs):
        return self.callable(*args, **kwargs)

    # TODO: deal with async calls


class EntrypointsRegistry:
    def __init__(self, app):
        self.app = app

    def _add_namespace(self, namespace):
        setattr(self, namespace, EntrypointsRegistry(self.app))

    def _add_entrypoint(self, name, cls):
        setattr(self, name, self._entrypoint_decorator(cls))

    def _entrypoint_decorator(self, cls):
        def wrapper(fn):
            # register the decorated function as a node
            return self.app.new_node(cls, fn)
        return wrapper


class UFW:
    def __init__(self):
        self.running = False
        self.entrypoints = set()
        self.entrypoint_registry = EntrypointsRegistry(self)

    def load_plugins(self):
        discovered_plugins = {
            name: importlib.import_module(name)
            for finder, name, ispkg
            in pkgutil.iter_modules()
            if name.startswith('ufw_')
        }
        for name, plugin in discovered_plugins.items():
            namespace = name[4:] # drop ufw_
            for entrypoint in getattr(plugin, []):
                if not hasattr(self.entrypoint_registry, namespace):
                    self.entrypoint_registry._add_namespace(namespace)
                getattr(self.entrypoint_registry, namespace)._add_entrypoint(
                    entrypoint["name"], entrypoint["class"]
                )

    def new_node(self, entrypoint, callable):
        # detect what this callable is
