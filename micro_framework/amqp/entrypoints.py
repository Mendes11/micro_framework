from logging import getLogger

from kombu import Exchange, Queue

from micro_framework.amqp.manager import ConsumerManager
from micro_framework.entrypoints import Entrypoint

logger = getLogger(__name__)


class BaseEventListener(Entrypoint):
    manager = ConsumerManager()
    exchange_type = 'direct'
    exchange_name = None
    queue_name = None
    event_name = None

    def get_exchange_name(self):
        return self.exchange_name

    def get_exchange(self):
        return Exchange(
            name=self.get_exchange_name(), type=self.exchange_type
        )

    def get_queue_name(self):
        return self.queue_name

    def get_event_name(self):
        return self.event_name

    def get_queue(self):
        return Queue(
            name=self.get_queue_name(), exchange=self.get_exchange(),
            routing_key=self.get_event_name(), durable=True
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


class EventListener(BaseEventListener):
    def __init__(self, source_service, event_name):
        self.source_service = source_service
        self.event_name = event_name

    def get_queue_name(self):
        service_name = self.runner.config['SERVICE_NAME']
        target_fn = self.route.function_path.split('.')[-1]
        return f'{self.source_service}.{self.event_name}_{service_name}.{target_fn}'

    def get_exchange_name(self):
        return self.source_service
