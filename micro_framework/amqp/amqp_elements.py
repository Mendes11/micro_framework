import uuid

from kombu import Queue, Exchange, Connection

from micro_framework.targets import Target


rpc_exchange_name = "ufw-rpc"


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
    return Queue(
        f"ufw-reply", exchange=exchange,
        routing_key=f"ufw-reply-{uuid.uuid4()}", message_ttl=3600 * 24,
        auto_delete=True
    )


default_transport_options = {
    'confirm_publish': True
}


def get_connection(amqp_uri):
    connection = Connection(
        amqp_uri, transport_options=default_transport_options,
        heartbeat=120
    )
    return connection