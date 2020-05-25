import logging
from concurrent.futures._base import wait
from threading import Lock

from amqp import PreconditionFailed
from functools import partial
from kombu import Connection, Consumer
from kombu.mixins import ConsumerMixin

from micro_framework.exceptions import ExtensionIsStopped, PoolStopped
from micro_framework.extensions import Extension

logger = logging.getLogger(__name__)


class ConsumerManager(Extension, ConsumerMixin):
    def __init__(self):
        self.queues = {}
        self._consumers = []
        self.run_thread = None
        self.connection = None
        # Lock due to message object apparently not being thread-safe...
        # TODO How to transform it in thread-safe? Substituting threading by
        #  gevent?
        self.message_lock = Lock()

    @property
    def amqp_uri(self):
        return self.runner.config.get('AMQP_URI')

    def on_connection_error(self, exc, interval):
        logger.info(
            "Error connecting to broker at {} ({}).\n"
            "Retrying in {} seconds.".format(self.amqp_uri, exc, interval))

    def get_connection(self):
        if not self.connection:
            return Connection(self.amqp_uri, heartbeat=120)
        return self.connection

    def setup(self):
        self.connection = self.get_connection()

    def start(self):
        self.run_thread = self.runner.spawn_extension(self, self.run)

    def stop(self):
        logger.debug("AMQP ConsumerManager is stopping")
        self.should_stop = True
        wait([self.run_thread])
        self.connection.close()
        logger.debug("AMQP ConsumerManager stopped.")

    def add_entrypoint(self, entrypoint):
        queue = entrypoint.queue
        self.queues[queue] = entrypoint

    def get_consumers(self, Consumer, channel):
        for queue, entrypoint in self.queues.items():
            callback = partial(self.on_message, entrypoint)
            self._consumers.append(
                Consumer(
                    queues=[queue],
                    callbacks=[callback],no_ack=False,
                    prefetch_count=self.runner.config.get("MAX_WORKERS", 1)
                )
            )
        return self._consumers

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        logger.debug(f'consumer started {consumers}')
        self.connection.connect()

    def on_message(self, entrypoint, body, message):
        logger.debug("Message Received")
        if not self.should_stop:
            try:
                return entrypoint.new_entry(message, body)
            except (ExtensionIsStopped, PoolStopped):
                pass
        message.requeue()

    def ack_message(self, message):
        with self.message_lock:  # One message at a time to prevent errors.
            message.ack()

    def requeue_message(self, message):
        with self.message_lock:
            message.requeue()
