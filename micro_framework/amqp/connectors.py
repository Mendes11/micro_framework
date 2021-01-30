import json
import uuid

from kombu import Queue

from micro_framework.amqp.amqp_elements import rpc_exchange, \
    rpc_routing_key, rpc_broadcast_routing_key, Publisher
from micro_framework.amqp.manager import RPCManager
from micro_framework.amqp.rpc import listen_to_correlation, lock
from micro_framework.rpc import RPCConnector, RPCConnection


class RPCProducer(Publisher, RPCConnection):
    """
    RPCProducer Implements the RPCConnection abstract class methods.

    At each "send" method, it will acquire a new connection from the
    producers pool (kombu library) and then send a message.

    It receives a send_to_listener callable, that is responsible to notify
    the EventListener of a new correlation_id to be aware of.
    """

    def __init__(
            self, amqp_uri, target_service: str, reply_to_queue: Queue,
            **kwargs
    ):
        super(RPCProducer, self).__init__(amqp_uri, **kwargs)
        self.target_service = target_service
        self.reply_to_queue = reply_to_queue

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
        with lock:
            # The publisher was raising some errors regarding wrong message
            # codes. Probably due to some concurrent call.
            # TODO This shouldn't have happened since we instantiate a new
            #  connection each time and use the get_producers internally....
            #  That uses the producers pool with acquire(block=True)
            self.publish(
                payload, routing_key=routing_key, exchange=exchange,
                **kwargs
            )

    def send_and_receive(self, *args, **kwargs):
        corr_id = str(uuid.uuid4())
        timeout = kwargs.pop("timeout", None)

        # Notify our reply listener of a new correlation_id that will have
        # ListenerReceiver.
        receiver = listen_to_correlation(corr_id)
        self.send(
            *args, correlation_id=corr_id,
            reply_to=self.reply_to_queue.routing_key,
            mandatory=True,

            **kwargs
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
        return RPCProducer(
            self.amqp_uri, reply_to_queue=self.reply_to_queue,
            target_service=self.target_service
        )
