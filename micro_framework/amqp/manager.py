import logging
from threading import Lock

from functools import partial
from kombu.mixins import ConsumerMixin
from kombu.pools import producers

from micro_framework.amqp.amqp_elements import rpc_queue, rpc_exchange, \
    rpc_broadcast_queue, get_connection
from micro_framework.exceptions import ExtensionIsStopped, PoolStopped, \
    RPCTargetDoesNotExist
from micro_framework.extensions import Extension
from micro_framework.rpc import RPCManagerMixin, format_rpc_response

logger = logging.getLogger(__name__)


class ConsumerManager(Extension, ConsumerMixin):
    def __init__(self):
        self.queues = {}
        self._consumers = []
        self.run_thread = None
        self.connection = None
        # Lock due to message object apparently not being thread-safe...
        self.message_lock = Lock()
        self.started = False

    context_singleton = True

    @property
    def amqp_uri(self):
        return self.runner.config['AMQP_URI']

    def on_connection_error(self, exc, interval):
        logger.info(
            "Error connecting to broker at {} ({}).\n"
            "Retrying in {} seconds.".format(self.amqp_uri, exc, interval))

    async def get_connection(self):
        if not self.connection:
            self.connection = get_connection(self.amqp_uri)
        return self.connection

    async def setup(self):
        self.connection = await self.get_connection()

    async def start(self):
        if not self.started:
            self.started = True
            await self.runner.spawn_extension(self, self.run)

    async def stop(self):
        if self.started:
            logger.debug("AMQP ConsumerManager is stopping")
            self.should_stop = True
            self.connection.close()
            logger.debug("AMQP ConsumerManager stopped.")
            self.started = False

    async def add_entrypoint(self, entrypoint):
        queue = entrypoint.queue
        self.queues[queue] = entrypoint

    def get_consumers(self, Consumer, channel):
        internal_queues = set()
        internal_callbacks = set()
        normal_queues = set()
        normal_callbacks = set()

        for queue, entrypoint in self.queues.items():
            callback = partial(self.on_message, entrypoint)
            if entrypoint.internal:
                internal_queues.add(queue)
                internal_callbacks.add(callback)
            else:
                normal_queues.add(queue)
                normal_callbacks.add(callback)

        self._consumers = [
            Consumer(
                queues=normal_queues,
                callbacks=normal_callbacks,
                no_ack=False,
                prefetch_count=self.runner.max_workers
            ),
            Consumer(
                queues=internal_queues,
                callbacks=internal_callbacks,
                no_ack=False,
            )
        ]
        return self._consumers

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        logger.debug(f'consumer started {consumers}')
        self.connection.connect()

    async def handle_new_message(self, entrypoint, body, message):
        try:
            await entrypoint.call_route(message, body)
        except Exception:
            logger.exception("")
            await self.requeue_message(message)
        else:
            await self.ack_message(message)

    def on_message(self, entrypoint, body, message):
        logger.debug("Message Received")
        if not self.should_stop:
            self.runner.execute_async(
                self.handle_new_message(entrypoint, body, message)
            )
        else:
            message.requeue()

    async def ack_message(self, message):
        # TODO using async we won't need this lock anymore. After all
        #  convertion is completed, remove this and test it.
        with self.message_lock:  # One message at a time to prevent errors.
            message.ack()

    async def requeue_message(self, message):
        with self.message_lock:
            message.requeue()


class RPCManager(RPCManagerMixin, ConsumerMixin):
    def __init__(self):
        super(RPCManager, self).__init__()
        self.connection = None
        self.started = False
        self.message_lock = Lock()

    context_singleton = True

    @property
    def amqp_uri(self):
        return self.runner.config['AMQP_URI']

    async def get_connection(self):
        if not self.connection:
            self.connection = get_connection(self.amqp_uri)
        return self.connection

    async def setup(self):
        self.connection = await self.get_connection()

    async def start(self):
        if not self.started:
            self.started = True
            await self.runner.spawn_extension(self, self.run)

    async def stop(self):
        if self.started:
            logger.debug("AMQP RPCManager is stopping")
            self.should_stop = True
            self.connection.close()
            logger.debug("AMQP RPCManager stopped.")
            self.started = False

    def get_consumers(self, Consumer, channel):
        exchange = rpc_exchange()
        queues = set()
        callbacks = set()
        broadcast_queue = rpc_broadcast_queue(
            self.runner.config["SERVICE_NAME"]
        )
        for target_id, entrypoint in self.entrypoints.items():
            callback = partial(self.on_message, entrypoint)
            queue = rpc_queue(
                self.runner.config["SERVICE_NAME"], target_id, exchange
            )
            queues.add(queue)
            callbacks.add(callback)

        self._consumers = [
            Consumer(
                queues=queues,
                callbacks=callbacks,
                no_ack=False,
                prefetch_count=self.runner.max_workers
            ),
            Consumer(
                queues=[broadcast_queue],
                callbacks=[self.on_broadcast_message],
                no_ack=False,
            )
        ]

        return self._consumers

    async def send_reply(self, message, payload):
        """
        Send to the RPC Response to the Reply Queue
        :param message:
        :param future:
        :return:
        """
        exchange = rpc_exchange()
        routing_key = message.properties["reply_to"]
        correlation_id = message.properties["correlation_id"]
        def dispatch():
            connection = get_connection(self.amqp_uri)
            with producers[connection].acquire(block=True) as producer:
                return producer.publish(
                    payload, exchange=exchange, routing_key=routing_key,
                    correlation_id=correlation_id, retry=True
                )
        await self.runner.event_loop.run_in_executor(None, dispatch)

    async def handle_new_call(self, target_ids, body, message):
        """
        Async Method to handle all procedures when receiving a RPC call.
        """
        try:
            # We verify if this callback is the one that handles the given
            # target_ids
            routing_key = message.delivery_info["routing_key"]
            target_id = routing_key.split(".", 1)[1]
            if "_broadcast." in target_id:
                target_id = target_id.split("_broadcast.", 1)[1]

            if target_id in target_ids:
                response = await self.consume_message(message, body)
            else:
                response = format_rpc_response(
                    None, exception=RPCTargetDoesNotExist(
                        "This target does not exist in the called RPC method"
                    )
                )
            await self.send_reply(message, response)
            await self.ack_message(message)
        except Exception:
            logger.exception("")
            await self.requeue_message(message)

    def on_message(self, entrypoint, body, message):
        logger.debug("AMQP RPC Message Received")
        if not self.should_stop:
            try:
                # Calling the async method to handle the new message.
                target_ids = [str(entrypoint.route.target)]
                self.runner.execute_async(
                    self.handle_new_call(target_ids, body, message)
                )
                return
            except (ExtensionIsStopped, PoolStopped):
                pass
        message.requeue()

    def on_broadcast_message(self, body, message):
        logger.debug("AMQP RPC Broadcast Message Received")
        if not self.should_stop:
            try:
                # Calling the async method to handle the new message.
                self.runner.execute_async(
                    self.handle_new_call(self.internal_commands, body, message)
                )
                return
            except (ExtensionIsStopped, PoolStopped):
                pass

        message.requeue()

    async def ack_message(self, message):
        message.ack()

    async def requeue_message(self, message):
        message.requeue()
