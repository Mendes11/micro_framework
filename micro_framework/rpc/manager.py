import json
import logging

from micro_framework.exceptions import RPCTargetDoesNotExist, \
    RPCMalformedMessage
from micro_framework.extensions import Extension
from .formatters import format_rpc_response

logger = logging.getLogger(__name__)

class RPCRegister:
    def __init__(self):
        self._targets = {}

    async def add_entrypoint(self, entrypoint):
        target = await entrypoint.route.target.target_detail()
        self._targets[target["target"]] = entrypoint

    async def remove_entrypoint(self, entrypoint):
        target = await entrypoint.route.target.target_detail()
        self._targets.pop(target["target"], None)

    async def clear(self):
        self._targets = {}

    @property
    def entrypoints(self):
        return list(self.targets.values())

    @property
    def targets(self):
        return self._targets


class RPCManager(Extension):
    """
    Manages RPC messages to call Entrypoints

    Messages using this Manager should have the following json schema:

    {
        "command": str,
        "command_args": list,
        "command_kwargs": dict,
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
        super(RPCManager, self).__init__()
        self.entrypoints = {}

    context_singleton = True
    internal_commands = {"__list_targets__", "__get_target__"}
    register = RPCRegister()  # This is unique amongst all RPCManagers.

    async def add_entrypoint(self, entrypoint):
        await self.register.add_entrypoint(entrypoint)
        detail = await entrypoint.route.target.target_detail()
        target_id = detail['target']
        if target_id in self.entrypoints:
            raise ValueError(
                f"Entrypoint {entrypoint} is already registered. "
                f"You cannot have multiple RPC entrypoints to the same "
                f"target."
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
                msg = "RPCManager received unknown message structure: {}".format(
                        message
                    )
                logger.error(msg)
                return format_rpc_response(None, RPCMalformedMessage(msg))

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
                   await entrypoint.route.target.target_detail()
                   for entrypoint in self.register.entrypoints
               ], None

    async def __get_target__(self, client, target):
        """
        Return details from an specific entrypoint target.
        :param client: RPC Client
        :param target: target rpc identification
        :return dict, Optional[Exception]: Target detail
        """
        entrypoint = self.register.targets[target]
        return await entrypoint.route.target.target_detail(), None

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
