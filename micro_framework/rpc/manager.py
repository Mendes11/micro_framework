import inspect
import json
import logging

from micro_framework.exceptions import RPCTargetDoesNotExist
from micro_framework.extensions import Extension
from .formatters import format_rpc_response
from ..entrypoints import Entrypoint

logger = logging.getLogger(__name__)

async def target_detail(entrypoint: Entrypoint):
    """
    Return the Details of an entrypoint's target.
    :param Entrypoint entrypoint: Entrypoint
    :return dict: Entrpoint Details dict.
    """
    target = entrypoint.route.target
    target_id = str(target)

    target_parameters = target.parameters

    args = [
        name for name, parameter in target_parameters.items() if
        parameter.kind in [
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.VAR_POSITIONAL
        ] and parameter.default is inspect._empty  # arguments with
        # default are considered POSITIONAL_OR_KEYWORD. So here I'll
        # filter only those that don't have a default value.
    ]
    kwargs = [
        name for name, parameter in target_parameters.items() if
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


class RPCRegister:
    def __init__(self):
        self._targets = {}

    async def add_entrypoint(self, entrypoint):
        target = await target_detail(entrypoint)
        self._targets[target["target"]] = entrypoint

    async def remove_entrypoint(self, entrypoint):
        target = await target_detail(entrypoint)
        self._targets.pop(target["target"], None)

    @property
    def entrypoints(self):
        return list(self.targets.values())

    @property
    def targets(self):
        return self._targets


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

    context_singleton = True
    internal_commands = {"__list_targets__", "__get_target__"}
    register = RPCRegister()  # This is unique amongst all RPCManagers.

    async def add_entrypoint(self, entrypoint):
        await self.register.add_entrypoint(entrypoint)
        detail = await target_detail(entrypoint)
        target_id = detail['target']
        if target_id in self.entrypoints:
            raise ValueError(
                f"Entrypoint {entrypoint} is already registered. "
                f"You cannot have multiple entrypoints to the same target"
            )
        self.entrypoints[target_id] = entrypoint

    async def _parse_message(self, client, command, *args, **kwargs):
        try:
            method = getattr(self, command)
            response, exception = await method(client, *args, **kwargs)
        except AttributeError:
            response, exception = await self.__call_target__(
                client, command, *args, **kwargs
            )
        return response, exception

    async def consume_message(self, client, message):
        """
        Consume the message, returning the method based on the rpc command.

        If the command is to call a route, then it should return None,
        since the route will spawn a worker, running in parallel.

        :param client: Client to identify the caller
        :param str message: RPC Message
        :return str: RPC Response
        """
        if not isinstance(message, dict):
            try:
                message = json.loads(message)
            except json.JSONDecodeError as exc:
                logger.error(
                    "RPCManager received unknown message structure: {}".format(
                        message
                    )
                )
                return format_rpc_response(None, exc)

        command = message['command']
        command_args = message.get('command_args', [])
        command_kwargs = message.get('command_kwargs', {})

        try:
            response, exception = await self._parse_message(
                client, command, *command_args, **command_kwargs
            )
        except Exception as exc:
            response = None
            exception = exc

        return format_rpc_response(response, exception)

    async def call_entrypoint(self, client, entrypoint, *args, **kwargs):
        return await entrypoint.call_route(client, *args, **kwargs)

    async def __list_targets__(self, client):
        """
        List all available targets by iterating through the entrypoints in
        the register.
        :param client: RPC Client
        :return list, Optional[Exception]: All available Entrypoints's target
        """
        return [
                   await target_detail(entrypoint)
                   for entrypoint in self.register.entrypoints
               ], None

    async def __get_target__(self, client, target):
        """
        Return details from an specific entrypoint target.
        :param client: RPC Client
        :param target: target rpc identification
        :return dict, Optional[Exception]: Target detail
        """
        return await target_detail(self.register.targets[target]), None

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
        worker = await self.call_entrypoint(
            client, entrypoint, *args, **kwargs
        )

        data = worker.result
        exception = worker.exception
        return data, exception
