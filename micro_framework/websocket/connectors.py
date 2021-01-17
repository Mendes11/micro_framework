from contextlib import contextmanager

import websockets
from websocket import create_connection, WebSocket

from micro_framework.exceptions import FrameworkException
from micro_framework.rpc import RPCConnection
from micro_framework.rpc.connectors import RPCConnector


class ServerDisconnected(FrameworkException):
    pass


class WSConnection(RPCConnection):
    def __init__(self, uri, timeout):
        self.uri = uri
        self.timeout = timeout

    @contextmanager
    def connection(self):
        con = create_connection(
            self.uri, timeout=self.timeout, enable_multithread=True
        )
        yield con
        con.close()

    def send(self, *args, **kwargs):
        with self.connection() as con:
            con.send(*args, **kwargs)

    def send_and_receive(self, payload, *args, **kwargs):
        with self.connection() as con:
            con.send(payload, *args, **kwargs)
            return con.recv()


class WebSocketConnector(RPCConnector):
    def __init__(self, uri, timeout=None):
        self.uri = uri
        self.timeout = timeout

    def get_connection(self) -> RPCConnection:
        return WSConnection(self.uri, self.timeout)
