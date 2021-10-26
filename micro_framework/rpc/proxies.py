from concurrent.futures._base import Future
from typing import Dict, Set, Any, Type, Callable, Optional

from .client import RPCClient
from .formatters import format_rpc_command


class RPCTarget:
    """
    Represents a Route's target from the connected server.

    It exposes methods to call the target synchronously or asynchronously.

    """

    def __init__(
            self,
            target_id: str,
            service: 'RPCService',
            target_details: Dict = None,
            parent: 'RPCTarget' = None,
    ):
        # Target ID may be given as the whole path and could have nested
        # content on it
        target_id, *nested = target_id.split(".", 1)

        self._target_id = target_id
        self._service = service
        self.target_args = None
        self.target_kwargs = None
        self.target_doc = None
        self._parent = parent

        if nested:
            # create and add the nested target
            nested_target = self._service._new_rpc_target(
                nested[0], target_details, parent=self
            )
            setattr(self, nested_target._target_id, nested_target)

        elif target_details:
            self.target_args = target_details.get("args")
            self.target_kwargs = target_details.get("kwargs")
            self.target_doc = target_details.get("docstring")

    def call_async(self, *args, **kwargs) -> Future:
        """
        Start a new thread to call the target.

        It returns a Future object with result/done methods.

        :return Future: Future object with result
        """
        return self._service._client.call_target_async(
            self.target_id, *args, _wait_response=True, **kwargs
        )

    @property
    def info(self):
        return f"""
RPC Target Object.
> Target: {self.target_id}
> Target's Arguments: {self.target_args}
> Target's Key Arguments: {self.target_kwargs}
> Target's Docstring: \n{self.target_doc}
        """

    @property
    def target_id(self):
        if self._parent:
            parent_path = self._parent.target_id
            return "{}.{}".format(parent_path, self._target_id)
        return self._target_id

    def __call__(self, *args, **kwargs) -> Any:
        future = self.call_async(*args, **kwargs)
        return future.result()

    def __repr__(self):
        return f"RPC<{self.target_id}>"

    def __str__(self):
        return self.__repr__()

    def __getattr__(self, item):
        if not item.startswith("__"):
            nested_target = self._service._new_rpc_target(
                item, None, parent=self
            )
            setattr(self, nested_target.target_id, nested_target)
            return nested_target
        raise AttributeError(item)


class RPCService:
    """
    RPC caller that represents a single service. It is used to call any
    function/method exposed as RPC on that service.

    Whenever an attribute from this class is requested and it is not
    present, we will automatically infer that it is a call to a target
    whose id is the attribute name and then return a :class:RPCTarget for
    that service.

    Targets can be either a Class, a Function or a Method of a class

    On instantiation a rpc_target_factory callable must be given. It should
    expect a target_id string and optionally the target details as a dict
    and a parent RPCTarget (for nested targets such as class -> method).

    If no target detail is given (None Type) then it is probably a target
    called without prior information (through getattr).

    """

    def __init__(
            self,
            service_name: str,
            rpc_target_factory: Callable[
                [str, 'RPCService', Optional[Dict], Optional[RPCTarget]], 'RPCTarget'],
            client: RPCClient
    ):
        self._targets = set()
        self.service_name = service_name
        self._target_factory = rpc_target_factory
        self._client = client

    def _new_rpc_target(self, target_id, target_details, parent):
        return self._target_factory(target_id, self, target_details, parent)

    def _add_target(self, target_id, target_details: dict = None) -> RPCTarget:
        obj = self._new_rpc_target(target_id, target_details, None)
        setattr(self, obj.target_id, obj)
        self._targets.add(obj)
        return obj

    def discover_targets(self):
        targets = self._client.list_targets()
        for target in targets:
            self._add_target(target["target"], target)

    def __getattr__(self, target_id):
        if not target_id.startswith("__"):
            return self._add_target(target_id)
        raise AttributeError(target_id)

    @property
    def targets(self) -> Set[RPCTarget]:
        return self._targets

    def __repr__(self):
        return f"RPC Service<{self.service_name}>"

    def __str__(self):
        return self.__repr__()


class RPCSystem:
    """
    RPC caller that represents the whole system. It is used to call any
    service that implements our rpc system.

    Whenever an attribute from this class is requested and it is not
    present, we will automatically infer that it is a call to a service
    whose name is the attribute name and then return a :class:RPCService for
    that service.
    """

    def __init__(
            self,
            service_proxy_factory: Callable[[str], Type[RPCService]],
    ):
        self.new_service_proxy = service_proxy_factory
        self._service_proxies = {}

    def __getattr__(self, service_name):
        if not service_name.startswith("__"):
            if not service_name in self._service_proxies:
                proxy = self.new_service_proxy(service_name)
                self._service_proxies[service_name] = proxy
            return self._service_proxies[service_name]
        raise AttributeError(service_name)
