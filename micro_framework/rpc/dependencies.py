import json

from micro_framework.dependencies import Dependency, RunnerDependency
from micro_framework.rpc.connectors import RPCConnector
from .formatters import format_rpc_command
from .proxies import RPCProxy


class RPCDependencyMixin:
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


class RPCDependency(RPCDependencyMixin, RunnerDependency):
    """
    Dependency Provider that injects a RPCProxy into the worker's target.
    """
    proxy_class: RPCProxy = RPCProxy
    connector_class: RPCConnector = None

    def get_connector_kwargs(self):
        """
        Key arguments to be passed to the connector init method
        :return dict:
        """
        return {}

    def get_connector(self) -> RPCConnector:
        return self.connector_class(**self.get_connector_kwargs())

    def get_proxy_class(self):
        return self.proxy_class

    def get_proxy_kws(self):
        return {
            "connector": self.get_connector(),
            "targets": self.list_targets() or [],
            "name": None
        }

    def get_proxy(self):
        return self.get_proxy_class()(**self.get_proxy_kws())

    async def get_dependency(self, worker):
        # Run in executor because the RPCProxy is not an async class and
        # will block the event loop.
        return await self.runner.event_loop.run_in_executor(
            None, self.get_proxy
        )
