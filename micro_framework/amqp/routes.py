import time
from concurrent.futures._base import wait
from logging import getLogger

from functools import partial
from kombu import Connection, Exchange, Queue
from kombu.mixins import ConsumerMixin

from micro_framework.extensions import Route, Extension

logger = getLogger(__name__)


class ConsumerManager(Extension, ConsumerMixin):
    def __init__(self):
        self.queues = {}
        self._consumers = []
        self.run_thread = None

    @property
    def amqp_uri(self):
        return self.runner.config.get('AMQP_URI')

    @property
    def connection(self):
        return Connection(self.amqp_uri, heartbeat=4)

    def on_connection_error(self, exc, interval):
        logger.info(
            "Error connecting to broker at {} ({}).\n"
            "Retrying in {} seconds.".format(self.amqp_uri, exc, interval))

    def start(self):
        self.run_thread = self.runner.extension_spawner.spawn(self.run, None)

    def stop(self):
        self.should_stop = True
        wait([self.run_thread])

    def add_route(self, route):
        queue = route.queue
        self.queues[queue] = route

    def bind(self, runner):
        self.runner = runner

    def get_consumers(self, Consumer, channel):
        for queue, route in self.queues.items():
            callback = partial(self.on_message, route)
            self._consumers.append(
                Consumer(queues=[queue], callbacks=[callback]))
        return self._consumers

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        logger.debug(f'consumer started {consumers}')

    def on_message(self, route, body, message):
        logger.debug("Message Received")

        def route_callback(future):
            future.result()
            message.ack()

        route.run(callback=route_callback, payload=body)


class EventRoute(Route):
    def __init__(self, source_service, event_name, target_fn_path):
        super(EventRoute, self).__init__(target_fn_path)
        self.source_service = source_service
        self.event_name = event_name

    manager = ConsumerManager()

    def run(self, payload, callback):
        self.call_function(payload, callback=callback)

    @property
    def queue_name(self):
        service_name = self.runner.config['SERVICE_NAME']
        target_fn = self.function_path.split('.')[-1]
        return f'{self.source_service}.{self.event_name}_{service_name}.{target_fn}'

    @property
    def exchange_name(self):
        return self.source_service

    def setup(self):
        self.exchange = Exchange(name=self.source_service, type='direct')
        self.queue = Queue(name=self.queue_name, exchange=self.exchange,
                           routing_key=self.event_name)
        self.queue.maybe_bind(self.manager.connection)
        self.queue.declare()
        self.manager.add_route(self)
