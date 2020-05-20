import logging
from concurrent.futures._base import wait

from functools import partial
from kombu import Connection
from kombu.mixins import ConsumerMixin

from micro_framework.exceptions import ExtensionIsStopped
from micro_framework.extensions import Extension

logger = logging.getLogger(__name__)


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
        return Connection(self.amqp_uri, heartbeat=50)

    def on_connection_error(self, exc, interval):
        logger.info(
            "Error connecting to broker at {} ({}).\n"
            "Retrying in {} seconds.".format(self.amqp_uri, exc, interval))

    def start(self):
        self.run_thread = self.runner.spawn_extension(self, self.run, None)

    def stop(self):
        logger.debug("AMQP ConsumerManager is stopping")
        self.should_stop = True
        wait([self.run_thread])
        self.connection.close()
        logger.debug("AMQP ConsumerManager stopped.")

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
        if not self.should_stop:
            try:
                route.on_message(payload=body, message=message)
            except ExtensionIsStopped:
                pass

    def acknowledge_message(self, message):
        message.ack()
