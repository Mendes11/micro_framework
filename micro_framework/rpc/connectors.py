from contextlib import contextmanager

from abc import ABC, abstractmethod
from typing import Union


class RPCConnection(ABC):
    """
    Abstract Class representing an instantiated connection.
    """

    @abstractmethod
    def send(self, message: Union[str, bytes], **kwargs):
        """
        Send a message without waiting for an answer.
        """
        ...

    @abstractmethod
    def send_and_receive(
            self, message: Union[str, bytes], **kwargs
    ) -> Union[str, bytes]:
        """
        Send a message and wait for the connected server response.
        """
        ...


class RPCConnector(ABC):
    """
    Interface to obtain a new connection.

    Each time the get_connection method is called, it should provide a new
    connection instance yielding it.

    All connection handling should be done inside the get_connection method,
    since this might not be thread-safe.
    """

    @abstractmethod
    @contextmanager
    def get_connection(self) -> RPCConnection:
        """
        Returns an open connection that implements RPCConnection.
        """
        ...
