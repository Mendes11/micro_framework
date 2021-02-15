import json
import threading
from concurrent.futures._base import Future
from typing import Dict, Set, Any, Type

from .connectors import RPCConnector
from .formatters import parse_rpc_response, format_rpc_command

FUNCTION_TYPE = "function"
CLASS_TYPE = "class"
METHOD_TYPE = "method"


class RPCTarget:
    """
    Represents a Target from the connected server.

    The target can be:
        . Function
        . Class
        . Class Method

    It exposes methods to call the target synchronously or asynchronously.

    """
    def __init__(
            self, connector: RPCConnector, target, target_name, args=None,
            kwargs=None, docstring=None, target_type=FUNCTION_TYPE,
            callable=True
    ):
        self.connector = connector
        self.target_id = target
        self.target_args = args
        self.target_kwargs = kwargs
        self.target_doc = docstring
        self.target_type = target_type
        self.name = target_name
        self.callable = callable # It can be a class that has methods to be called instead of calling the class itself.
        self._inner_targets = {}

    def add_inner_target(self, target: "RPCTarget"):
        self._inner_targets[target.name] = target
        setattr(self, target.name, target)

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
        if not self.callable:
            raise NotImplementedError(
                "The RPC {} {} is not callable. Please use .targets to list it's "
                "inner targets that are probably the callable ones."
            )
        message = format_rpc_command(self.target_id, *args, **kwargs)
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
    def targets(self):
        if self._inner_targets:
            return list(self._inner_targets.items())

    @property
    def info(self):
        return f"""
RPC Target Object.
> Target: {self.target_id}
> Target's Arguments: {self.target_args}
> Target's Key Arguments: {self.target_kwargs}
> Target's Docstring: \n{self.target_doc}
        """

    def __call__(self, *args, **kwargs) -> Any:
        if self.callable:
            future = self.call_async(*args, **kwargs)
            return future.result()
        raise NotImplementedError(
            "The RPC {} {} is not callable. Please use .targets to list it's "
            "inner targets that are probably the callable ones."
        )

    def __repr__(self):
        return f"RPC<{self.target_type}: {self.target_id}>"

    def __str__(self):
        return self.__repr__()


class RPCMixin:
    """
    Mixin with methods to communicate with RPCManager and exposing the
    available commands.
    """

    def parse_response(self, response):
        """
        Parses the response from the RPC Manager.
        :param str response: RPC Manager Response String
        :return dict|list: Response Data.
        """
        if isinstance(response, str):
            return json.loads(response).get('data', None)
        return response

    def make_call(self, message, wait_response=True):
        """
        Call the RPC Manager using the instantiated client.

        :param dict|str message: Message to be Sent.
        :param bool wait_response: If True, then we wait for a reply.
        :return json|list: Manager Response
        """
        if isinstance(message, dict):
            message = json.dumps(message)
        connection = self.get_connector().get_connection()
        if wait_response:
            return self.parse_response(connection.send_and_receive(message))

        # Otherwise
        connection.send(message)

    def list_targets(self):
        """
        Call __list_targets__ command. As the name suggests, it retrieves all
        registered RPC targets in the connected server.
        :return list: List of Targets
        """
        message = format_rpc_command('__list_targets__')
        response = self.make_call(message)
        return response

    def get_target(self, target):
        """
        Call __get_target__(target) command.

        It returns a single target information.

        :param str target: The target name.
        :return dict: Target information
        """
        message = format_rpc_command('__get_target__', target)
        response = self.make_call(message)
        return response


class RPCServiceProxy(RPCMixin):
    """
    Populates itself based on the list of targets from the connected RPC server.

    The class object attributes are the RPC Server targets instantiated as a
    RPCTarget class object.

    """
    target_class: Type[RPCTarget] = RPCTarget
    connector_class: RPCConnector = RPCConnector

    def __init__(self, service_name: str = None):
        self._targets = set()
        self.name = service_name
        self.connector = self.get_connector()
        self.update_targets()

    def update_targets(self):
        self._targets = set()
        targets = self.list_targets()
        for target in targets:
            self.add_target(target)

    def get_connector_kwargs(self) -> Dict:
        return {}

    def get_connector(self):
        return self.connector_class(**self.get_connector_kwargs())

    def get_target_class(self):
        return self.target_class

    def get_target_kws(self, **target_data) -> Dict:
        return {
            "connector": self.connector,
            **target_data,
        }

    def new_function_target(
            self, target_data: dict, target_name, target_type=FUNCTION_TYPE
    ) -> RPCTarget:
        kws = self.get_target_kws(**target_data)
        kws["target_type"] = target_type
        kws["target_name"] = target_name
        return self.get_target_class()(**kws)

    def new_class_target(self, cls_name: str) -> RPCTarget:
        return self.get_target_class()(
            self.connector,
            target=cls_name,
            target_name=cls_name,
            callable=False,
            target_type=CLASS_TYPE
        )

    def add_target(self, target: dict) -> RPCTarget:
        target_name, *nested = target['target'].split('.', 1)
        if nested:
            obj = getattr(
                self, target_name, self.new_class_target(target_name)
            )
            method_target = self.new_function_target(
                target, nested[0], target_type=METHOD_TYPE
            )
            obj.add_inner_target(method_target)
        else:
            obj = self.new_function_target(target, target_name)
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


class RPCProxy:
    """
    Proxy that returns a RPCServiceProxy class based on the service called.

    Whenever a user tries to get an attribute from this class that is not
    present on it, we will automatically infer that it is a call to a service
    whose name is the attribute name and then return a RPCServiceProxy for
    that service.
    """
    def __init__(self):
        self._service_proxies = {}

    service_proxy_class: RPCServiceProxy = RPCServiceProxy

    def get_service_proxy_kwargs(self, service_name):
        return {
            "service_name": service_name
        }

    def get_service_proxy(self, service_name):
        return self.service_proxy_class(
            **self.get_service_proxy_kwargs(service_name)
        )

    def __getattr__(self, service_name):
        if not service_name in self._service_proxies:
            proxy = self.get_service_proxy(
                service_name=service_name
            )
            self._service_proxies[service_name] = proxy
        return self._service_proxies[service_name]
