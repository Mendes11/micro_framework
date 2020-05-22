from logging import getLogger

from kombu import Exchange, Queue

from micro_framework.amqp.manager import ConsumerManager
from micro_framework.entrypoints import Entrypoint

logger = getLogger(__name__)


class EventListener(Entrypoint):
    def __init__(self, source_service, event_name, route):
        super(EventListener, self).__init__(route)
        self.source_service = source_service
        self.event_name = event_name

    manager = ConsumerManager()

    def setup(self):
        super(EventListener, self).setup()
        self.exchange = Exchange(
            name=self.source_service, type='direct'
        )
        self.queue = Queue(
            name=self.queue_name, exchange=self.exchange,
            routing_key=self.event_name, durable=True
        )
        self.queue.maybe_bind(self.manager.get_connection())
        self.queue.declare()
        self.manager.add_entrypoint(self)

    def on_finished_route(self, entry_id, worker):
        # We ack the message independently of the result.
        self.manager.ack_message(entry_id)

    def on_failure(self, entry_id):
        # Something not related to the business logic went wrong.
        self.manager.requeue_message(entry_id)

    @property
    def queue_name(self):
        service_name = self.runner.config['SERVICE_NAME']
        target_fn = self.route.function_path.split('.')[-1]
        return f'{self.source_service}.{self.event_name}_{service_name}.{target_fn}'

    @property
    def exchange_name(self):
        return self.source_service
