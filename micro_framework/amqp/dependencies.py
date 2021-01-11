import multiprocessing
import uuid

from functools import partial
from typing import Callable, Dict
from uuid import UUID

from kombu import Exchange, producers

from micro_framework.amqp.amqp_elements import rpc_exchange, rpc_reply_queue, \
    get_connection
from micro_framework.amqp.connectors import AMQPRPCConnector, \
    ListenerReceiver
from micro_framework.amqp.entrypoints import BaseEventListener
from micro_framework.dependencies import Dependency
from micro_framework.rpc import RPCDependency


def send_to_listener(
        correlation_id: UUID, queue: multiprocessing.Queue,
        lock: multiprocessing.RLock
) -> ListenerReceiver:
    """
    Sends through a Queue the correlation_id and a connection which
    the Listener will send the message that matches the correlation_id.

    The use of the Queue is necessary because we might use
    multiprocess as a worker spawner and the listener will be running in
     the main process but the RPCTarget caller won't.

    :param UUID correlation_id: A UUID V4 representing a RPC Message
    through the AMQP Broker.
    """

    receiver, sender = multiprocessing.Pipe(duplex=False)
    with lock:
        queue.put(
            (sender, correlation_id)
        )
    print("send_to_listener: Inserting the received correlation_id: {} with "
          "sender: {} to the queue: {}".format(correlation_id, sender, queue))
    listener_receiver = ListenerReceiver(receiver)
    return listener_receiver


class Producer(Dependency):
    def __init__(self, confirm_publish=True, service_name=None):
        self.service_name = service_name
        self.confirm_publish = confirm_publish

    @property
    def exchange(self):
        service_name = self.service_name or self.config['SERVICE_NAME']
        exchange_name = f"{service_name}.events"  # Nameko Compatible
        return Exchange(exchange_name, type='topic', auto_delete=True)

    @property
    def amqp_uri(self):
        return self.config['AMQP_URI']

    @property
    def transport_options(self):
        return {
            'confirm_publish': self.confirm_publish
        }

    async def get_dependency(self, worker):
        exchange = self.exchange

        def publish(event_name, payload):
            return dispatch(
                self.amqp_uri, exchange, event_name, payload,
            )

        return publish

m = multiprocessing.Manager()
_correlations_queue = m.Queue()
lock = m.Lock()

# Map the correlation IDs to send to their respective Connections (
# Through a multiprocessing PIPE)
_correlation_ids = m.dict()


def listen_to_correlation(correlation_id):
    global _correlation_ids
    with lock:
        receiver, sender = multiprocessing.Pipe(duplex=False)
        _correlation_ids[correlation_id] = sender
        return ListenerReceiver(receiver)


class RPCReplyListener(BaseEventListener):
    """
    Listens to any RPCReply for this service
    """
    singleton = True
    internal = True

    def __init__(self, *args, **kwargs):
        super(RPCReplyListener, self).__init__(*args, **kwargs)
        # This will be shared between this listener and the
        # DependencyProviders that implement it, so that they can send
        # through the Queue the Correlation and Connection they should listen
        # to and then send through.
        # self.m = multiprocessing.Manager()
        # self._correlations_queue = self.m.Queue()
        # self.lock = self.m.Lock()
        #
        # # Map the correlation IDs to send to their respective Connections (
        # # Through a multiprocessing PIPE)
        # self._correlation_ids = self.m.dict()


    async def setup(self):
        self._reply_queue = rpc_reply_queue()
        self.routing_key = self._reply_queue.routing_key
        await super(RPCReplyListener, self).setup()

    async def get_exchange(self):
        return rpc_exchange()

    async def get_queue(self):
        return self._reply_queue

    # def listen_to_correlation(self, correlation_id):
    #     receiver, sender = multiprocessing.Pipe(duplex=False)
    #     self._correlation_ids[correlation_id] = sender
    #     return ListenerReceiver(receiver)

    async def read_correlations_queue(self):
        global _correlation_ids
        print("Reading from correlations_queue: {}".format(_correlations_queue))
        try:
            while not _correlations_queue.empty():
                sender, corr_id = _correlations_queue.get_nowait()
                with lock:
                    _correlation_ids[corr_id] = sender
        except Exception as exc:
            # TODO Remove Prints
            import traceback; traceback.print_exc()
            print("Error reading correlations_queue")

    async def call_route(self, entry_id, *args, _meta=None, **kwargs):
        global _correlation_ids
        # Any exception here will make the manager requeue the message
        # First we update our correlation_ids dict by emptying the
        # correlations_queue.
        # await self.read_correlations_queue()
        try:
            corr_id = entry_id.properties["correlation_id"]
        except KeyError:
            # TODO Remove Print
            print("Received a Message without correlation_id")
            return

        body = args[0]
        try:
            if corr_id in _correlation_ids:
                with lock:
                    sender = _correlation_ids.pop(corr_id)
                if not sender.closed and sender.writable:
                    sender.send(body)
                else:
                    print("Sender is closed or not writable.{}, {}".format(
                        sender.closed, sender.writable))
            else:
                print("Correlation id: {} not found on _correlation_ids "
                      "dict.".format(corr_id))
        except Exception:
            print("Exception at correlation checking.")
            import traceback; traceback.print_exc()

    # @property
    # def correlations_queue(self):
    #     return self._correlations_queue


class RPCProxyProvider(RPCDependency):
    """
    Provides a RPCProxy with AMQPRPCConnector.
    """

    def __init__(self, target_service):
        self.target_service = target_service
        super(RPCProxyProvider, self).__init__()

    reply_listener = RPCReplyListener()
    connector_class = AMQPRPCConnector

    def get_connector_kwargs(self) -> Dict:
        print("Instantiating Provider with correlations_queue = {}".format(
            _correlations_queue))

        return {
            "amqp_uri": self.config.get("AMQP_URI"),
            "target_service": self.target_service,
            "reply_to_queue": self.reply_listener.queue,
            # "send_to_listener": partial(
            #     send_to_listener, queue=self.reply_listener.correlations_queue,
            #     lock=self.reply_listener.lock
            # )
        }


def dispatch(amqp_uri, exchange, routing_key, payload, **kwargs):
    """
    Helper to dispatch a single payload that will load the client and call
    the send method.

    :param str amqp_uri:
    :param Exchange exchange:
    :param str routing_key:
    :param Any payload:
    :param dict headers:
    """
    connection = get_connection(amqp_uri)
    with producers[connection].acquire(block=True) as producer:
        return producer.publish(
            payload, exchange=exchange, routing_key=routing_key,
            **kwargs
        )