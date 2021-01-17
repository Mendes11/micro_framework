import json
import uuid

from kombu import Queue, Connection

from micro_framework.amqp.amqp_elements import rpc_exchange, \
    rpc_routing_key, rpc_broadcast_routing_key, default_transport_options
from micro_framework.amqp.manager import RPCManager
from micro_framework.amqp.rpc import Publisher
from micro_framework.rpc import RPCConnector, RPCConnection


class ListenerReceiver:
    """
    Implements a method to receive from the Reply EventListener's connection
    instance with a timeout possibility.

    When the timeout is reached, it raises TimeoutError.

    This class is usually created by the RPCConnection when it sends a new
    rpc message to the broker and notify the Reply-Listener to send the reply
    to a specific multiprocess.Connection instance.

    """
    def __init__(self, connection):
        self.connection = connection

    def result(self, timeout=None):
        if timeout is not None:
            if self.connection.poll(timeout=timeout):
                return self.connection.recv()
            raise TimeoutError()
        while not self.connection.poll(timeout=.5):
            pass
        return self.connection.recv()


class RPCProducer(RPCConnection):
    """
    RPCProducer Implements the RPCConnection abstract class methods.

    At each "send" method, it will acquire a new connection from the
    producers pool (kombu library) and then send a message.

    It receives a send_to_listener callable, that is responsible to notify
    the EventListener of a new correlation_id to be aware of.
    """
    def __init__(
            self, producer, target_service: str, reply_to_queue: Queue
    ):
        self.target_service = target_service
        self.reply_to_queue = reply_to_queue
        self.producer = producer

    def send(self, payload, *args, **kwargs):
        payload = json.loads(payload)
        exchange = rpc_exchange()
        target_id = payload["command"]
        if target_id in RPCManager.internal_commands:
            routing_key = rpc_broadcast_routing_key(
                self.target_service, target_id
            )
        else:
            routing_key = rpc_routing_key(
                self.target_service, target_id=target_id
            )
        self.producer.publish(
            payload, *args, routing_key=routing_key, exchange=exchange,
            **kwargs
        )

    def send_and_receive(self, *args, **kwargs):
        corr_id = str(uuid.uuid4())
        timeout = kwargs.pop("timeout", None)

        from micro_framework.amqp.dependencies import listen_to_correlation
        # Add this correlation_id to the dict of correlations and their senders
        receiver = listen_to_correlation(corr_id)
        self.send(
            *args, correlation_id=corr_id,
            reply_to=self.reply_to_queue.routing_key, **kwargs
        )
        result = receiver.result(timeout=timeout)
        return result


class AMQPRPCConnector(RPCConnector):
    """

    Instantiate and returns a Publisher when get_connection method is called.

    The Arguments are:
        .amqp_uri: The Connection String to the broker.

        .reply_to_queue: The queue to which we will tell the RPCServer to
        send the response message.
    """
    def __init__(
            self, amqp_uri: str, target_service: str, reply_to_queue: Queue
    ):
        self.amqp_uri = amqp_uri
        self.target_service = target_service
        self.reply_to_queue = reply_to_queue

    def get_connection(self):
        return Publisher(
            self.amqp_uri, reply_to_queue=self.reply_to_queue,
            target_service=self.target_service
        )
