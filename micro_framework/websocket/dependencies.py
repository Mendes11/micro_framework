from micro_framework.rpc import RPCDependency
from micro_framework.websocket.client import WebSocketClient


class WebSocketRPCClient(RPCDependency):
    client_class = WebSocketClient

    def __init__(self, address, port):
        self.address = address
        self.port = port

    def get_client_kwargs(self):
        return {'address': self.address, 'port': self.port}
