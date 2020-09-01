from logging import getLogger

from kombu import Exchange, Queue

from micro_framework.amqp.manager import ConsumerManager
from micro_framework.entrypoints import Entrypoint

logger = getLogger(__name__)


class BaseEventListener(Entrypoint):
    """
    Base class to handle Queue consuming of a single routing key in an exchange.
    It declares a Queue, binded to the exchange with exchange_name and with a
    routing_key from routing_key attribute.
    """
    manager = ConsumerManager()
    exchange_type = 'topic'
    exchange_name = None
    routing_key = None

    def get_exchange(self):
        """
        Return the Exchange that will have a queue binded to.
        :return Exchange:
        """
        return Exchange(
            name=self.exchange_name, type=self.exchange_type,
            auto_delete=True
        )

    def get_queue_name(self):
        """
        Return the name of the queue to be binded.
        It usually must be unique by consuming entrypoint to avoid the same
        queue being consumed by multiple different routes.
        :return str: The name of the Queue
        """
        return f'{self.exchange_name}_{self.routing_key}_queue'

    def get_queue(self):
        """
        Returns the Queue to be created or loaded using the exchange and
        routing key declared.
        :return:
        """
        return Queue(
            name=self.get_queue_name(), exchange=self.get_exchange(),
            routing_key=self.routing_key, durable=True
        )

    def setup(self):
        super(BaseEventListener, self).setup()
        self.exchange = self.get_exchange()
        self.queue = self.get_queue()
        self.queue.maybe_bind(self.manager.get_connection())
        self.queue.declare()
        self.manager.add_entrypoint(self)

    def on_finished_route(self, entry_id, worker):
        # We ack the message independently of the result.
        self.manager.ack_message(entry_id)

    def on_failure(self, entry_id):
        # Something not related to the business logic went wrong.
        self.manager.requeue_message(entry_id)

    def new_entry(self, message, payload):
        """
        Called by the Manager when a new message is received.
        :param Message message: Message object
        :param dict|str payload: Event Payload
        """
        self.call_route(message, payload)


class QueueListener(BaseEventListener):
    def __init__(self, exchange_name, routing_key, queue_name=None,
                 exchange_type='topic'):
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.exchange_type = exchange_type
        self.queue_name = queue_name

    def get_queue_name(self):
        if self.queue_name:
            return self.queue_name
        return super(QueueListener, self).get_queue_name()


class EventListener(QueueListener):
    def __init__(self, source_service, event_name):
        self.source_service = source_service
        self.event_name = event_name
        exchange_name = f'{source_service}.events'  # Nameko Compatible!
        super(EventListener, self).__init__(
            exchange_name=exchange_name, routing_key=event_name
        )

    def get_queue_name(self):
        service_name = self.runner.config['SERVICE_NAME']
        target_fn = self.route.target_path.split('.')[-1]
        if self.route.method_name:
            target_fn = f"{target_fn}.{self.route.method_name}"
        # source_service.something_happened__my_service.target.function
        return f'{self.source_service}.{self.event_name}__{service_name}.{target_fn}'
