import inspect
import json
import logging
import threading

from abc import ABC, abstractmethod
from concurrent.futures._base import Future

from micro_framework.dependencies import Dependency
from micro_framework.exceptions import RPCTargetDoesNotExist, \
    RPCException
from micro_framework.extensions import Extension

logger = logging.getLogger(__name__)


def import_exception(exception):
    if exception['exception'] in __builtins__:
        return __builtins__[exception['exception']](exception['message'])
    return RPCException(exception)


def format_rpc_command(command, *args, **kwargs):
    """
    Returns a json str with the expected structure to communicate with our
    RPCManager.

    :param str command: RPC Command. Can be either list/get targets or a call
    to the target by its name.
    :param args: Arguments to be sent to the RPC Command.
    :param kwargs: Key Arguments to be sent to the RPC Command.
    :return str: JSON Command
    """
    return json.dumps({
        'command': command, 'command_args': args, 'command_kwargs': kwargs
    })


def format_rpc_response(data, exception=None):
    """
    Formats a response from a RPC Manager.
    It provides the data and/or a serialized exception so it can be
    re-created by the caller.

    :param Any data: A JSON Serializable object.
    :param Exception exception: An Exception object
    :return str: JSON Response.
    """
    exception_data = None
    if exception:
        exception_data = {
            'exception': type(exception).__name__,
            'message': str(exception),
        }
    return json.dumps({
        'data': data,
        'exception': exception_data
    })


def parse_rpc_response(message):
    """
    Reads a response returned by the RPC Manager.
    It returns either a loaded JSON or an exception if the exception key is
    not None.

    :param str message: JSON Response
    :return dict|list|Exception: Loaded RPC Response.
    """
    response = json.loads(message)
    if response.get('exception'):
        return import_exception(response['exception'])
    return response.get('data')


class RPCConnection(ABC):
    """
    Abstract Class representing a connection handler.

    The RPCMixin and RPCTarget will use it as a context manager to open a new
    connection and communicate.
    """

    @abstractmethod
    def __enter__(self):
        """
        Return a connection that implements the methods send and rcv.
        :return:
        """
        ...

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        This method should close the opened connection.
        """
        ...


class RPCClient:
    """
    Client that
    """

    def __init__(self, connection_class, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.connection_class = connection_class

    def get_connection(self):
        return self.connection_class(*self.args, **self.kwargs)


class RPCManagerMixin(Extension):
    """
    Manages RPC messages to call Entrypoints

    Messages using this Manager should have the following json schema:

    {
        "command": str,
        "command_args": list
    }


    Available Commands are:

     - __list_targets__
        Lists all possible targets with their parameters and docstring.
        - args: None
        - kwargs: None

     - __get_target__:
        Retrieve detail from a specific target
        - args: target str
        - kwargs: None

     - <target>
        Call the specified target, triggering the Route
        - args: Arguments for the specific target
        - kwargs: Kwargs for the specific target.

    """

    def __init__(self):
        super(RPCManagerMixin, self).__init__()
        self.entrypoints = {}

    def add_entrypoint(self, entrypoint):
        target_detail = self.target_detail(entrypoint)
        target_id = target_detail['target']
        if target_id in self.entrypoints:
            raise ValueError(
                f"Entrypoint {entrypoint} is already registered. "
                f"You cannot have multiple entrypoints to the same target"
            )
        self.entrypoints[target_id] = entrypoint

    def _parse_message(self, client, command, *args, **kwargs):
        try:
            method = getattr(self, command)
            response = method(client, *args, **kwargs)
        except AttributeError:
            response = self.__call_target__(client, command, *args, **kwargs)
        return response

    def consume_message(self, client, message):
        """
        Consume the message, returning the method based on the rpc command.

        If the command is to call a route, then it should return None,
        since the route will spawn a worker, running in parallel.

        :param client: Client to identify the caller
        :param str message: RPC Message
        :return str: RPC Response
        """
        message = json.loads(message)
        command = message['command']
        command_args = message.get('command_args', [])
        command_kwargs = message.get('command_kwargs', {})
        try:
            response = self._parse_message(
                client, command, *command_args, **command_kwargs
            )
        except Exception as exc:
            return format_rpc_response(None, exc)
        return format_rpc_response(response)

    def target_detail(self, entrypoint):
        """
        Return the Details of an entrypoint's target.
        :param Entrypoint entrypoint: Entrypoint
        :return dict: Entrpoint Details dict.
        """
        target = entrypoint.route.target
        if inspect.isclass(target):
            # ID is ClassName.method_name
            target_id = target.__name__
            target = getattr(target, '__call__')
            if entrypoint.route.method_name:
                # User set method_name, so we use it instead of __call__
                target_id = f"{target.__name__}.{entrypoint.route.method_name}"
                target = getattr(target, entrypoint.route.method_name)

        elif inspect.isfunction(target):
            # ID is function_name
            target_id = target.__name__

        else:
            # Target is a str path
            target_id = target.split('.')[-1]
            if entrypoint.route.method_name:
                # it has a method_name, so we add it to the target_id
                target_id += f'.{entrypoint.route.method_name}'
            return {
                'target': target_id,
                'args': 'not_available',
                'kwargs': 'not_available',
                'docstring': 'not_available'
            }
        signature = inspect.signature(target)
        args = [
            name for name, parameter in signature.parameters.items() if
            parameter.kind in [
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.VAR_POSITIONAL
            ] and parameter.default is inspect._empty  # arguments with
            # default are considered POSITIONAL_OR_KEYWORD. So here I'll
            # filter only those that don't have a default value.
        ]
        kwargs = [
            name for name, parameter in signature.parameters.items() if
            parameter.kind in [
                inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.VAR_KEYWORD,
                inspect.Parameter.POSITIONAL_OR_KEYWORD
            ] and (parameter.default is not inspect._empty or
                   parameter.kind is inspect.Parameter.VAR_KEYWORD)
        ]
        return {
            'target': target_id,
            'args': args,
            'kwargs': kwargs,
            'docstring': inspect.getdoc(target)
        }

    def __list_targets__(self, client):
        """
        List all available targets by iterating through the entrypoints.
        :param client: RPC Client
        :return list: All available Entrypoints's target
        """
        return [
            self.target_detail(entrypoint)
            for entrypoint in self.entrypoints.values()
        ]

    def __get_target__(self, client, target):
        """
        Return details from an specific entrypoint target.
        :param client: RPC Client
        :param target: target rpc identification
        :return dict: Target detail
        """
        return self.target_detail(self.entrypoints[target])

    def __call_target__(self, client, target, *args, **kwargs):
        """
        Call the entrypoint's route by using the target name.
        :param client: RPC Client
        :param str target: Entrypoint Identification
        :param dict data:
        :return:
        """
        if target not in self.entrypoints:
            raise RPCTargetDoesNotExist(
                f"{target} does not exist or is not exposed through a "
                f"RPC method."
            )
        entrypoint = self.entrypoints[target]
        self.call_entrypoint(client, entrypoint, *args, **kwargs)


class AsyncRPCManagerMixin(RPCManagerMixin):
    async def _parse_message(self, client, command, *args, **kwargs):
        try:
            method = getattr(self, command)
            response = method(client, *args, **kwargs)
        except AttributeError:
            response = await self.__call_target__(
                client, command, *args, **kwargs
            )
        return response

    async def consume_message(self, client, message):
        """
        Consume the message, returning the method based on the rpc command.

        If the command is to call a route, then it should return None,
        since the route will spawn a worker, running in parallel.

        :param client: Client to identify the caller
        :param str message: RPC Message
        :return str: RPC Response
       """
        message = json.loads(message)
        command = message['command']
        command_args = message.get('command_args', [])
        command_kwargs = message.get('command_kwargs', {})
        try:
            response = await self._parse_message(
                client, command, *command_args, **command_kwargs
            )
        except Exception as exc:
            return format_rpc_response(None, exc)
        if response:
            response = format_rpc_response(response)
        return response

    async def __call_target__(self, client, target, *args, **kwargs):
        """
        Call the entrypoint's route by using the target name.
        :param client: RPC Client
        :param str target: Entrypoint Identification
        :param dict data:
        :return:
        """
        if target not in self.entrypoints:
            raise RPCTargetDoesNotExist(
                f"{target} does not exist or is not exposed through a "
                f"RPC method."
            )
        entrypoint = self.entrypoints[target]
        await self.call_entrypoint(client, entrypoint, *args, **kwargs)


class RPCTarget:
    """
    Represents a Route's target from the connected server.

    It exposes methods to call the target synchronously or asynchronously.

    """
    def __init__(self, client: RPCClient, target, args=None, kwargs=None,
                 docstring=None):
        self.client = client
        self.target = target
        self.target_args = args
        self.target_kwargs = kwargs
        self.target_doc = docstring

    @staticmethod
    def _call_client(message: str, client: RPCClient, future: Future):
        with client.get_connection() as connection:
            connection.send(message)
            message = connection.recv()
        result = parse_rpc_response(message)
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

        #FIXME This is smelly... how is it usually done? Maybe use the runner
        # spawners to do it, but then issue #12 should be done first.
        # TODO Change to executor to use the submit method and it will
        #  automatically handle the Future class.
        t = threading.Thread(
            target=self._call_client, args=(message, self.client, future)
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

    def __call__(self, *args, **kwargs):
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
    def __init__(self, client, targets, name=None):
        self._targets = set()
        self.client = client
        self.name = name

        for target in targets:
            self._targets.add(self.add_target(target))

    def add_target(self, target, nested_name=None):
        target_name = nested_name or target['target']
        target_name, *nested = target_name.split('.')
        if nested:
            obj = getattr(self, target_name, RPCProxy(
                self.client, [], name=target_name))
            obj.add_target(target, '.'.join(nested))
        else:
            obj = RPCTarget(self.client, **target)
        setattr(self, target_name, obj)
        return obj

    @property
    def targets(self):
        return self._targets

    def __repr__(self):
        name = self.name or 'Server'
        return f"RPC<{name}>"

    def __str__(self):
        return self.__repr__()


class RPCDependencyMixin:
    """
    Mixin with methods to communicate with RPCManager and exposing the
    available commands.

    It expects a client_class that implements RPCClient which will be used as a
    context manager for each connection.
    """

    connection_class: RPCConnection = None

    def get_connection_kwargs(self):
        """
        Key arguments to be passed to the client_class instantiation
        :return dict:
        """
        return {}

    def get_client(self):
        """
        Returns an instance of client_class.
        :return RPCClient: The Client object
        """
        return RPCClient(self.connection_class, **self.get_connection_kwargs())

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
        with self.get_client().get_connection() as client:
            client.send(message)
            return self.parse_response(client.recv()) if wait_response else None

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


class RPCDependency(RPCDependencyMixin, Dependency):
    """
    Dependency Provider that injects a RPCProxy into the worker's target.

    """
    def setup(self):
        super(RPCDependencyMixin, self).setup()
        self.targets = self.list_targets() or []

    def get_dependency(self, worker):
        return RPCProxy(self.get_client(), self.targets)
