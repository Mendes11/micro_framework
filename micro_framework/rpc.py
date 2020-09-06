import asyncio
import inspect
import json
import logging
import traceback

from micro_framework.exceptions import RPCTargetDoesNotExist
from micro_framework.extensions import Extension

logger = logging.getLogger(__name__)


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

    def format_rpc_response(self, data, exception=None):
        exception_data = None
        if exception:
            exception_data = {
                'exception': exception.__class__.__name__,
                'message': str(exception),
                'traceback': traceback.format_exc()
            }
        return json.dumps({
            'data': data,
            'exception': exception_data
        })

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
            return self.format_rpc_response(None, exc)
        return self.format_rpc_response(response)

    def target_detail(self, entrypoint):
        """
        Return the Details of an entrypoint's target.
        :param Entrypoint entrypoint: Entrypoint
        :return dict: Entrpoint Details dict.
        """
        target = entrypoint.route.target
        if inspect.isclass(target):
            target_id = f"{target.__name__}.{entrypoint.route.method_name}"
            target = getattr(target, entrypoint.route.method_name)
        elif inspect.isfunction(target):
            target_id = target.__name__
        else:
            # str path
            return {
                'target': target.split('.')[-1],
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
            return self.format_rpc_response(None, exc)
        if response:
            response = self.format_rpc_response(response)
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
