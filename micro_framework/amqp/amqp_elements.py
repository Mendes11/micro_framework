import uuid
from contextlib import contextmanager

import warnings

from amqp import ChannelError
from kombu import Queue, Exchange, Connection, producers

from micro_framework.exceptions import RPCTargetDoesNotExist
from micro_framework.targets import Target

rpc_exchange_name = "ufw-rpc"
DEFAULT_TRANSPORT_OPTIONS = {
    'max_retries': 3,
    'interval_start': 2,
    'interval_step': 1,
    'interval_max': 5
}
DEFAULT_RETRY_POLICY = {'max_retries': 3}
DEFAULT_HEARTBEAT = 120
DEFAULT_SERIALIZER = 'json'
PERSISTENT = 2


def event_queue_name(event_name: str, source_service: str, service_name: str,
                     target: Target) -> str:
    """
    Returns the Queue name that should be used when listening to events from
    a publisher.

    :param str event_name:
    :param str source_service:
    :param str service_name:
    :param Target target:
    :return str: The Queue name
    """

    # source_service.something_happened__my_service.target.function
    return f'{source_service}.{event_name}__{service_name}.{target}'


def event_exchange_name(source_service: str) -> str:
    return f"{source_service}.events"


def rpc_queue_name(service_name, target_id):
    return "rpc-{}-{}".format(service_name, target_id)


def rpc_routing_key(service_name, target_id):
    return "{}.{}".format(service_name, target_id)


def get_rpc_target_id_from_routing_key(service_name, routing_key):
    _, target_id = routing_key.split("{}.".format(service_name))
    return target_id


def rpc_broadcast_routing_key(service_name, target_id):
    return "{}._broadcast.{}".format(service_name, target_id)


def rpc_broadcast_queue(service_name):
    return Queue(
        "rpc-{}-broadcast".format(service_name),
        exchange=rpc_exchange(),
        routing_key="{}._broadcast.*".format(service_name)
    )


def rpc_queue(service_name: str, target_id: str, exchange: Exchange) -> Queue:
    return Queue(
        rpc_queue_name(service_name, target_id),
        exchange=exchange,
        routing_key=rpc_routing_key(service_name, target_id)
    )


def rpc_exchange() -> Exchange:
    return Exchange(name=rpc_exchange_name, durable=True, type="topic")


def rpc_reply_queue() -> Queue:
    """
    Returns a Queue instance for RPC Replies.

    This Queue is Durable and each message will live for a day without being
    consumed.

    Since at each start we don't want to route messages to the wrong service,
    the routing key should be unique.

    :return Queue: The RPC Reply Queue.
    """
    exchange = rpc_exchange()
    name = f"ufw-reply-{uuid.uuid4()}"
    return Queue(
        name, exchange=exchange,
        routing_key=name, message_ttl=3600 * 24,
        expires=3600 * 12 # Stays 12 Hours before being deleted.
    )


default_transport_options = {
    'confirm_publish': True
}


def get_connection(amqp_uri, heartbeat=None, ssl=None, transport_options=None):
    if heartbeat is None: heartbeat = DEFAULT_HEARTBEAT
    if transport_options is None: transport_options = default_transport_options.copy()

    connection = Connection(
        amqp_uri, transport_options=transport_options,
        heartbeat=heartbeat, ssl=ssl
    )
    return connection


@contextmanager
def get_producer(
        amqp_uri, confirms=True, ssl=None, transport_options=None,
        heartbeat=None
):
    if transport_options is None:
        transport_options = DEFAULT_TRANSPORT_OPTIONS.copy()
    transport_options['confirm_publish'] = confirms
    conn = get_connection(
        amqp_uri, heartbeat=heartbeat, ssl=ssl,
        transport_options=transport_options
    )
    with producers[conn].acquire(block=True) as producer:
        yield producer


class Publisher:
    """
    Utility helper for publishing messages to RabbitMQ.

    ################### IMPORTANT NOTE #############################
    #                                                              #
    #   THIS CLASS WAS (NOT PROUDLY) TAKEN FROM NAMEKO PROJECT     #
    #                                                              #
    #    Don't know why, but my implementation wasn't              #
    #    working properly                                          #
    #                                                              #
    ################################################################

    ref: https://github.com/nameko/nameko/blob/a719cb1487f643769e2d13daf255c20551490f43/nameko/amqp/publish.py#L40
    """

    use_confirms = True
    """
    Enable `confirms <http://www.rabbitmq.com/confirms.html>`_ for this
    publisher.
    The publisher will wait for an acknowledgement from the broker that
    the message was receieved and processed appropriately, and otherwise
    raise. Confirms have a performance penalty but guarantee that messages
    aren't lost, for example due to stale connections.
    """

    transport_options = DEFAULT_TRANSPORT_OPTIONS.copy()
    """
    A dict of additional connection arguments to pass to alternate kombu
    channel implementations. Consult the transport documentation for
    available options.
    """

    delivery_mode = PERSISTENT
    """
    Default delivery mode for messages published by this Publisher.
    """

    mandatory = False
    """
    Require `mandatory <https://www.rabbitmq.com/amqp-0-9-1-reference.html
    #basic.publish.mandatory>`_ delivery for published messages.
    """

    priority = 0
    """
    Priority value for published messages, to be used in conjunction with
    `consumer priorities <https://www.rabbitmq.com/priority.html>_`.
    """

    expiration = None
    """
    `Per-message TTL <https://www.rabbitmq.com/ttl.html>`_, in milliseconds.
    """

    serializer = "json"
    """ Name of the serializer to use when publishing messages.
    Must be registered as a
    `kombu serializer <http://bit.do/kombu_serialization>`_.
    """

    compression = None
    """ Name of the compression to use when publishing messages.
    Must be registered as a
    `kombu compression utility <http://bit.do/kombu-compression>`_.
    """

    retry = True
    """
    Enable automatic retries when publishing a message that fails due
    to a connection error.
    Retries according to :attr:`self.retry_policy`.
    """

    retry_policy = DEFAULT_RETRY_POLICY
    """
    Policy to apply when retrying message publishes, if requested.
    See :attr:`self.retry`.
    """

    declare = []
    """
    Kombu :class:`~kombu.messaging.Queue` or :class:`~kombu.messaging.Exchange`
    objects to (re)declare before publishing a message.
    """

    def __init__(
            self, amqp_uri, use_confirms=None, serializer=None,
            compression=None, heartbeat=None,
            delivery_mode=None, mandatory=None, priority=None, expiration=None,
            declare=None, retry=None, retry_policy=None, ssl=None,
            **publish_kwargs
    ):
        self.amqp_uri = amqp_uri
        self.ssl = ssl
        self.heartbeat = heartbeat

        # publish confirms
        if use_confirms is not None:
            self.use_confirms = use_confirms

        # delivery options
        if delivery_mode is not None:
            self.delivery_mode = delivery_mode
        if mandatory is not None:
            self.mandatory = mandatory
        if priority is not None:
            self.priority = priority
        if expiration is not None:
            self.expiration = expiration

        # message options
        if serializer is not None:
            self.serializer = serializer
        if compression is not None:
            self.compression = compression

        # retry policy
        if retry is not None:
            self.retry = retry
        if retry_policy is not None:
            self.retry_policy = retry_policy

        # declarations
        if declare is not None:
            self.declare = declare

        # other publish arguments
        self.publish_kwargs = publish_kwargs

    def publish(self, payload, **kwargs):
        """ Publish a message.
        """
        publish_kwargs = self.publish_kwargs.copy()

        # merge headers from when the publisher was instantiated
        # with any provided now; "extra" headers always win
        headers = publish_kwargs.pop('headers', {}).copy()
        headers.update(kwargs.pop('headers', {}))
        headers.update(kwargs.pop('extra_headers', {}))

        use_confirms = kwargs.pop('use_confirms', self.use_confirms)
        transport_options = kwargs.pop('transport_options',
                                       self.transport_options
                                       )
        transport_options['confirm_publish'] = use_confirms

        delivery_mode = kwargs.pop('delivery_mode', self.delivery_mode)
        mandatory = kwargs.pop('mandatory', self.mandatory)
        priority = kwargs.pop('priority', self.priority)
        expiration = kwargs.pop('expiration', self.expiration)
        serializer = kwargs.pop('serializer', self.serializer)
        compression = kwargs.pop('compression', self.compression)
        retry = kwargs.pop('retry', self.retry)
        retry_policy = kwargs.pop('retry_policy', self.retry_policy)

        declare = self.declare[:]
        declare.extend(kwargs.pop('declare', ()))

        publish_kwargs.update(kwargs)  # remaining publish-time kwargs win

        with get_producer(self.amqp_uri,
                          use_confirms,
                          self.ssl,
                          transport_options,
                          heartbeat=self.heartbeat
                          ) as producer:
            try:
                producer.publish(
                    payload,
                    headers=headers,
                    delivery_mode=delivery_mode,
                    mandatory=mandatory,
                    priority=priority,
                    expiration=expiration,
                    compression=compression,
                    declare=declare,
                    retry=retry,
                    retry_policy=retry_policy,
                    serializer=serializer,
                    **publish_kwargs
                )
            except ChannelError as exc:
                if "NO_ROUTE" in str(exc):
                    raise RPCTargetDoesNotExist()
                raise

            if mandatory and not use_confirms:
                warnings.warn(
                    "Mandatory delivery was requested, but "
                    "unroutable messages cannot be detected without "
                    "publish confirms enabled."
                )
