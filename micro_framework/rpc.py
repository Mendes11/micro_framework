import logging

logger = logging.getLogger(__name__)


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


