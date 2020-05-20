from logging import getLogger

from functools import partial
from kombu import Exchange, Queue

from micro_framework.amqp.manager import ConsumerManager
from micro_framework.routes import Route

logger = getLogger(__name__)


class EventRoute(Route):
    def __init__(self, source_service, event_name, target_fn_path,
                 dependencies=None, translators=None):
        super(EventRoute, self).__init__(
            target_fn_path, dependencies, translators
        )
        self.source_service = source_service
        self.event_name = event_name

    manager = ConsumerManager()

    def setup(self):
        self.exchange = Exchange(name=self.source_service, type='direct')
        self.queue = Queue(name=self.queue_name, exchange=self.exchange,
                           routing_key=self.event_name, durable=True)
        self.queue.maybe_bind(self.manager.connection)
        self.queue.declare()
        self.manager.add_route(self)

    def on_success(self, result, **callback_kwargs):
        message = callback_kwargs['message']
        self.manager.acknowledge_message(message)

    def on_failure(self, exception, **callback_kwargs):
        message = callback_kwargs['message']
        self.manager.acknowledge_message(message)

    def on_message(self, payload, message):
        """
        Called by the manager when a message to this route is received.
        :param str payload: message content
        :param kombu.message message:
        """
        self.start_route(payload, callback_kwargs={'message': message})

    @property
    def queue_name(self):
        service_name = self.runner.config['SERVICE_NAME']
        target_fn = self.function_path.split('.')[-1]
        return f'{self.source_service}.{self.event_name}_{service_name}.{target_fn}'

    @property
    def exchange_name(self):
        return self.source_service

