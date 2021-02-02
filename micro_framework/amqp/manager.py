import logging
from threading import Lock

from functools import partial
from kombu import Consumer
from kombu.mixins import ConsumerMixin
from kombu.pools import producers

from micro_framework.amqp.amqp_elements import rpc_queue, rpc_exchange, \
    rpc_broadcast_queue, get_connection, Publisher, \
    get_rpc_target_id_from_routing_key
from micro_framework.amqp.exceptions import DuplicatedListener
from micro_framework.exceptions import ExtensionIsStopped, PoolStopped, \
    RPCTargetDoesNotExist
from micro_framework.extensions import Extension
from micro_framework.rpc import RPCManagerMixin, format_rpc_response

logger = logging.getLogger(__name__)


class ConsumerManager(Extension, ConsumerMixin):
    def __init__(self):
        self.queues = {}
        self._consumers = []
        self.entrypoint_map = {} # exchange.routing_key -> Entrypoint
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

    def _entrypoint_key(self, exchange, routing_key):
        return f"{exchange}.{routing_key}"

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

    def get_consumers(self, _, channel):
        internal_queues = set()
        normal_queues = set()

        for queue, entrypoint in self.queues.items():


            if entrypoint.internal:
                internal_queues.add(queue)
            else:
                normal_queues.add(queue)

            entrypoint_key = self._entrypoint_key(
                queue.exchange.name, queue.routing_key
            )
            if entrypoint_key in self.entrypoint_map:
                raise DuplicatedListener(
                    "You cannot define a listener to the same set "
                    "({}, {}) in the same RunnerContext.".format(
                        queue.exchange.name, queue.routing_key
                    )
                )
            self.entrypoint_map[entrypoint_key] = entrypoint

        self._consumers = [
            Consumer(
                channel,
                queues=normal_queues,
                callbacks=[self.on_message],
                no_ack=False,
                prefetch_count=self.runner.max_workers
            ),
            Consumer(
                channel.connection.channel(),
                queues=internal_queues,
                callbacks=[self.on_message],
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

    def on_message(self, body, message):
        logger.debug("Message Received")
        entrypoint_key = self._entrypoint_key(
            message.delivery_info["exchange"],
            message.delivery_info["routing_key"]
        )
        entrypoint = self.entrypoint_map.get(entrypoint_key)

        if entrypoint is None:
            message.ack()
            return

        if not self.should_stop:
            try:
                self.runner.execute_async(
                    self.handle_new_message(entrypoint, body, message),
                    raise_for_future=False
                )
            except (PoolStopped, ExtensionIsStopped):
                message.requeue()
        else:
            message.requeue()

    async def ack_message(self, message):
        message.ack()

    async def requeue_message(self, message):
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

    def get_consumers(self, _, channel):
        exchange = rpc_exchange()
        queues = set()
        broadcast_queue = rpc_broadcast_queue(
            self.runner.config["SERVICE_NAME"]
        )

        for target_id, entrypoint in self.entrypoints.items():
            queue = rpc_queue(
                self.runner.config["SERVICE_NAME"], target_id, exchange
            )
            queues.add(queue)

        self._consumers = [
            Consumer(
                channel,
                queues=queues,
                callbacks=[self.on_message],
                no_ack=False,
                prefetch_count=self.runner.max_workers
            ),
            Consumer(
                channel.connection.channel(), # Get new channel.
                queues=[broadcast_queue],
                callbacks=[self.on_broadcast_message],
                no_ack=False,
            )
        ]

        return self._consumers

    async def send_reply(self, message, payload):
        """
        Send the RPC Response to the Reply Queue
        :param message:
        :param future:
        :return:
        """
        exchange = rpc_exchange()
        routing_key = message.properties.get("reply_to")
        correlation_id = message.properties.get("correlation_id")
        if not routing_key or not correlation_id:
            # Nothing to do here. Might be some unknown message.
            return

        publisher = Publisher(self.amqp_uri)
        publish = partial(
            publisher.publish, payload, exchange=exchange,
            routing_key=routing_key, correlation_id=correlation_id, retry=True
        )
        await self.runner.event_loop.run_in_executor(None, publish)

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
        except Exception:
            logger.exception("Error on handling a RPCManager call")
            await self.requeue_message(message)
        else:
            await self.ack_message(message)

    def on_message(self, body, message):
        logger.debug("AMQP RPC Message Received")
        if not self.should_stop:
            try:

                target_id = get_rpc_target_id_from_routing_key(
                    self.runner.config["SERVICE_NAME"],
                    message.delivery_info["routing_key"]
                ) # We can infer the target by getting the routing key.

                # Calling the async method to handle the new message.
                self.runner.execute_async(
                    self.handle_new_call([target_id], body, message),
                    raise_for_future=False
                )
                return
            except (ExtensionIsStopped, PoolStopped):
                message.requeue()
        else:
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
                message.requeue()
        else:
            message.requeue()

    async def ack_message(self, message):
        message.ack()

    async def requeue_message(self, message):
        message.requeue()
