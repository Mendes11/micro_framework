import threading
from concurrent.futures._base import Future
from typing import Dict, List, Set, Any, Type

from .connectors import RPCConnector
from .formatters import parse_rpc_response, format_rpc_command


class RPCTarget:
    """
    Represents a Route's target from the connected server.

    It exposes methods to call the target synchronously or asynchronously.

    """

    def __init__(self, connector: RPCConnector, target, args=None, kwargs=None,
                 docstring=None):
        self.connector = connector
        self.target = target
        self.target_args = args
        self.target_kwargs = kwargs
        self.target_doc = docstring

    def call(self, message: str, connector: RPCConnector) -> Dict:
        connection = connector.get_connection()
        result_message = connection.send_and_receive(message)
        return parse_rpc_response(result_message)

    def _call_client_task(
            self, message: str, connector: RPCConnector, future: Future):
        result = self.call(message, connector)
        if isinstance(result, Exception):
            future.set_exception(result)
            return
        future.set_result(result)

    def call_async(self, *args, **kwargs) -> Future:
        """
        Start a new thread to call the target.

        It returns a Future object with result/done methods.

        :return Future: Future object with result
        """

        message = format_rpc_command(self.target, *args, **kwargs)
        future = Future()

        # TODO Create an executor instance in the worker context (specially
        #  for the instantiated worker).
        t = threading.Thread(
            target=self._call_client_task,
            args=(message, self.connector, future)
        )
        t.daemon = True
        t.start()

        return future

    @property
    def info(self):
        return f"""
RPC Target Object.
> Target: {self.target}
> Target's Arguments: {self.target_args}
> Target's Key Arguments: {self.target_kwargs}
> Target's Docstring: \n{self.target_doc}
        """

    def __call__(self, *args, **kwargs) -> Any:
        future = self.call_async(*args, **kwargs)
        return future.result()

    def __repr__(self):
        return f"RPC<{self.target}>"

    def __str__(self):
        return self.__repr__()


class RPCProxy:
    """
    Injected Dependency that populates itself based on the list of targets
    from the connected RPC server.

    The class object attributes are the RPC Server targets instantiated as a
    RPCTarget class object.

    """
    target_class = RPCTarget

    def __init__(self, connector: RPCConnector, targets: List[Dict],
                 name: str = None):
        self._targets = set()
        self.connector = connector
        self.name = name

        for target in targets:
            self.add_target(target)

    def get_target_class(self) -> Type[RPCTarget]:
        return self.target_class

    def get_target_kws(self, **target) -> Dict:
        return {
            "connector": self.connector,
            **target,
        }

    def get_target(self, **target) -> RPCTarget:
        return self.get_target_class()(**self.get_target_kws(**target))

    def new_nested_proxy(self, name: str) -> 'RPCProxy':
        return self.__class__(self.connector, [], name=name)

    def add_target(self, target: dict, nested_name: str = None) -> RPCTarget:
        target_name = nested_name or target['target']
        target_name, *nested = target_name.split('.', 1)
        if nested:
            obj = getattr(self, target_name, self.new_nested_proxy(target_name))
            obj.add_target(target, nested[0])
        else:
            obj = self.get_target(**target)
        setattr(self, target_name, obj)
        self._targets.add(obj)
        return obj

    @property
    def targets(self) -> Set[RPCTarget]:
        return self._targets

    def __repr__(self):
        name = self.name or 'Server'
        return f"RPC<{name}>"

    def __str__(self):
        return self.__repr__()
