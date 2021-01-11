import json
import uuid
from contextlib import contextmanager
from typing import Callable

from kombu import Queue, Connection
from kombu.pools import producers, connections

from micro_framework.amqp.amqp_elements import rpc_exchange, \
    rpc_routing_key, rpc_broadcast_routing_key, get_connection, \
    default_transport_options
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
            self, producer, target_service: str,
            reply_to_queue: Queue, #reply_listener,
            #send_to_listener: Callable
    ):
        self.target_service = target_service
        self.reply_to_queue = reply_to_queue
        # self.send_to_listener = send_to_listener
        #self.reply_listener = reply_listener
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
        print("Starting to send message with corr_id = {}".format(corr_id))
        # TODO There is a problem here with the Queue. Maybe because of the
        #  singleton stuff. Check it.

        # Notify our reply listener of a new correlation_id that will have
        # ListenerReceiver.
        # receiver = self.send_to_listener(corr_id)
        from micro_framework.amqp.dependencies import listen_to_correlation
        receiver = listen_to_correlation(corr_id)
        self.send(
            *args, correlation_id=corr_id,
            reply_to=self.reply_to_queue.routing_key, **kwargs
        )
        print("Message Sent. Waiting for result.")
        result = receiver.result(timeout=timeout)
        print("Received Result: {}".format(result))
        return result


class AMQPRPCConnector(RPCConnector):
    """

    Instantiate and yields a RPCProducer when get_connection method is called.

    The Arguments are:
        .amqp_uri: The Connection String to the broker.

        .reply_to_queue: The queue to which we will tell the RPCServer to
        send the response message.

        .send_to_listener: A callable that receives a correlation_id and will
        return a ListenerReceiver, that handles the listening from the
        reply queue.

    """
    def __init__(
            self, amqp_uri: str, target_service: str, reply_to_queue: Queue,
            # reply_listener, # send_to_listener: Callable
    ):
        self.amqp_uri = amqp_uri
        self.target_service = target_service
        self.reply_to_queue = reply_to_queue
        # self.send_to_listener = send_to_listener
        # self.reply_listener = reply_listener

    # @contextmanager
    def get_connection(self):
        # amqp_connection = get_connection(self.amqp_uri)
        transport_options = default_transport_options.copy()
        transport_options['confirm_publish'] = True
        conn = Connection(
            self.amqp_uri, transport_options=transport_options
        )
        return Publisher(self.amqp_uri, reply_to_queue=self.reply_to_queue,
                         target_service=self.target_service)
        # with producers[conn].acquire(block=True) as producer:
        #     yield RPCProducer(
        #         producer, self.target_service, self.reply_to_queue,
        #         # self.reply_listener
        #         # self.send_to_listener
        #     )
