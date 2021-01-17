from logging import getLogger
from typing import Any

from kombu import Exchange, Queue

from micro_framework.amqp.amqp_elements import event_queue_name, \
    event_exchange_name
from micro_framework.amqp.manager import ConsumerManager, RPCManager
from micro_framework.entrypoints import Entrypoint
from micro_framework.rpc.manager import target_detail

logger = getLogger(__name__)


class BaseEventListener(Entrypoint):
    """
    Base class to handle Queue consuming of a single routing key in an exchange.
    It declares a Queue, binded to the exchange with exchange_name and with a
    routing_key from routing_key attribute.
    """
    def __init__(self, exchange_name=None, routing_key=None,
                 exchange_type='topic', payload_filter=None):
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.exchange_type = exchange_type
        self.payload_filter = payload_filter

    manager = ConsumerManager()
    internal = False

    async def get_exchange(self):
        """
        Return the Exchange that will have a queue binded to.
        :return Exchange:
        """
        return Exchange(
            name=self.exchange_name, type=self.exchange_type,
            auto_delete=True
        )

    async def get_queue_name(self):
        """
        Return the name of the queue to be binded.
        It usually must be unique by consuming entrypoint to avoid the same
        queue being consumed by multiple different routes.
        :return str: The name of the Queue
        """
        return f'{self.exchange_name}_{self.routing_key}_queue'

    async def get_queue(self):
        """
        Returns the Queue to be created or loaded using the exchange and
        routing key declared.
        :return:
        """
        queue_name = await self.get_queue_name()
        exchange = await self.get_exchange()
        return Queue(
            name=queue_name, exchange=exchange,
            routing_key=self.routing_key, durable=True
        )

    async def setup(self):
        await super(BaseEventListener, self).setup()
        self.exchange = await self.get_exchange()
        self.queue = await self.get_queue()
        self.queue.maybe_bind(await self.manager.get_connection())
        self.queue.declare()
        await self.manager.add_entrypoint(self)

    async def call_route(self, entry_id, *args, _meta=None, **kwargs):
        """
        Called by the Manager when a new message is received.
        :param Message message: Message object
        :param dict|str payload: Event Payload
        """
        payload = args[0]
        if not self.payload_filter or self.payload_filter(payload):
            return await super(BaseEventListener, self).call_route(
                entry_id, *args, _meta=_meta, **kwargs
            )


class QueueListener(BaseEventListener):
    def __init__(self, *args, queue_name=None, **kwargs):
        super(QueueListener, self).__init__(*args, **kwargs)
        self.queue_name = queue_name

    async def get_queue_name(self):
        if self.queue_name:
            return self.queue_name
        return await super(QueueListener, self).get_queue_name()


class EventListener(QueueListener):
    def __init__(self, source_service, event_name, **kwargs):
        self.source_service = source_service
        self.event_name = event_name
        exchange_name = event_exchange_name(source_service)
        super(EventListener, self).__init__(exchange_name, event_name, **kwargs)

    async def get_queue_name(self):
        service_name = self.runner.config['SERVICE_NAME']
        return event_queue_name(
            self.event_name, self.source_service,
            service_name, self.route.target
        )


class RPCListener(Entrypoint):
    manager = RPCManager()

    async def setup(self):
        await super(RPCListener, self).setup()
        await self.manager.add_entrypoint(self)
