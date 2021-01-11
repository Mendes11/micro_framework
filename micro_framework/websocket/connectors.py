from contextlib import contextmanager

import websockets
from websocket import create_connection, WebSocket

from micro_framework.exceptions import FrameworkException
from micro_framework.rpc import RPCConnection
from micro_framework.rpc.connectors import RPCConnector


class ServerDisconnected(FrameworkException):
    pass


class WSConnection(WebSocket, RPCConnection):
    # def recv(self):
    #     res = super(WSConnection, self).recv()
    #     if not res and not self.connected:
    #         raise ServerDisconnected(
    #             f"Server has unexpectedly disconnected"
    #         )
    #     return res
    #
    # def send(self, payload, *args, **kwargs):
    #     length = super(WSConnection, self).send(payload, *args, **kwargs)
    #     if not length and not self.connected:
    #         raise ServerDisconnected(
    #             f"Server has unexpectedly disconnected"
    #         )
    #     return length

    def send_and_receive(self, payload, *args, **kwargs):
        self.send(payload, *args, **kwargs)
        return self.recv()


class WebSocketConnection(RPCConnector):
    def __init__(self, uri, timeout=None):
        self.uri = uri
        self.timeout = timeout

    @contextmanager
    def get_connection(self) -> RPCConnection:
        con = create_connection(
            self.uri, timeout=self.timeout, class_=WSConnection,
            enable_multithread=True
        )
        yield con
        con.close()

