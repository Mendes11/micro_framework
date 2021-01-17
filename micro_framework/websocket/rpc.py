import asyncio
import time

import websockets

import micro_framework.amqp.amqp_elements
from micro_framework.exceptions import MaxConnectionsReached
from micro_framework.rpc import RPCTarget, parse_rpc_response, RPCProxy


class WSRPCTarget(RPCTarget):
    def call(self, message, client):
        connection_successful = False
        result = None
        while not connection_successful:
            # Try connecting to a server with available workers.
            # This is useful when the WS Entrypoint is behind a Load Balancer
            # and therefore we have multiple WebSocket servers, with some empty
            # and others not.

            connection = self.connector.get_connection()
            result_message = connection.send_and_receive(message)
            result = parse_rpc_response(result_message)
            if isinstance(result, MaxConnectionsReached):
                time.sleep(0.5)  # TODO Is there any better way?
                continue  # Keep Trying

            connection_successful = True
        return result


class WSRPCProxy(RPCProxy):
    target_class = WSRPCTarget
