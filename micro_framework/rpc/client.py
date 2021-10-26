import json
from concurrent.futures import Future
from threading import Thread
from typing import List, Dict

from micro_framework.rpc import RPCConnector
from micro_framework.rpc.formatters import format_rpc_command, \
    parse_rpc_response


class RPCClient:
    """
    Interface with a server that implements RPC Through our RPCManager

    It exposes the methods available to communicate with another server
    using the Framework or at least the same API.

    This is an application layer class that implements the messaging system
    on top of a transport layer on how to communicate with another server.
    It's up to the given RPCConnector instance to know how to transport the
    message from the Application Layer to the wire.
    """
    def __init__(self, connector: RPCConnector):
        self.connector = connector

    def make_async_call(self, message, wait_response=True) -> Future:
        """
        Send the message non-blocking in a separate thread.

         :param dict|str message: Message to be Sent.
        :param bool wait_response: If True, then we wait for a reply.
        :return: A Concurrent Future object
        :rtype: concurrent.Future
        """
        future = Future()

        def call(future, make_call, message, wait_response):
            res = make_call(message, wait_response=wait_response)
            if isinstance(res, Exception):
                future.set_exception(res)
                return

            future.set_result(res)

        t = Thread(
            target=call, args=(future, self.make_call, message, wait_response),
            daemon=True
        )
        t.start()
        return future

    def make_call(self, message, wait_response=True):
        """
        Call the RPC Manager using the instantiated client.

        :param dict|str message: Message to be Sent.
        :param bool wait_response: If True, then we wait for a reply.
        :return json|list: Manager Response
        """
        if isinstance(message, dict):
            message = json.dumps(message)
        with self.connector.get_connection() as connection:
            if wait_response:
                return parse_rpc_response(
                    connection.send_and_receive(message)
                )

            # Otherwise
            connection.send(message)

    def list_targets(self) -> List:
        """
        Call __list_targets__ command. As the name suggests, it retrieves all
        registered RPC targets in the connected server.
        :return list: List of Targets
        """
        message = format_rpc_command('__list_targets__')
        return self.make_call(message)

    def list_targets_async(self) -> Future:
        """
        Call __list_targets__ command in a separate thread.
        As the name suggests, it retrieves all registered RPC targets in
        the connected server.
        :return list: List of Targets
        """
        message = format_rpc_command('__list_targets__')
        return self.make_async_call(message)

    def get_target(self, target) -> Dict:
        """
        Call __get_target__(target) command.

        It returns a single target information.

        :param str target: The target name.
        :return dict: Target information
        """
        message = format_rpc_command('__get_target__', target)
        return self.make_call(message)

    def get_target_async(self, target) -> Future:
        """
        Call __get_target__(target) command in a different Thread.

        It returns a Future whose result is a single target information.

        :param str target: The target name.
        :return dict: Target information
        """
        message = format_rpc_command('__get_target__', target)
        return self.make_async_call(message)

    def call_target(self, target, *args, _wait_response=True, **kwargs):
        """
        Call the target string passing it's arguments and key arguments
        :param str target: Target ID
        :param args: Target Args
        :param kwargs: Target Kwargs
        :return: Response
        """
        message = format_rpc_command(target, *args, **kwargs)
        return self.make_call(message, wait_response=_wait_response)

    def call_target_async(
            self, target, *args, _wait_response=True, **kwargs
    ) -> Future:
        """
        Call the target string in a separate Thread passing it's arguments and
        key arguments

        :param str target: Target ID
        :param args: Target Args
        :param kwargs: Target Kwargs
        :return: Response
        """
        message = format_rpc_command(target, *args, **kwargs)
        return self.make_async_call(message, wait_response=_wait_response)
