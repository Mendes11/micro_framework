import logging
from typing import Dict, Union

from amqp.exceptions import NotFound
from kombu import Exchange, producers

from micro_framework.amqp.amqp_elements import get_connection, Publisher
from micro_framework.amqp.connectors import AMQPRPCConnector
from micro_framework.amqp.rpc import RPCReplyListener
from micro_framework.dependencies import Dependency
from micro_framework.rpc import RPCServiceProvider, RPCProvider
from micro_framework.rpc.dependencies import RPCProviderFactories

logger = logging.getLogger(__name__)


class SystemProducer(Dependency):
    def __init__(self, confirm_publish=True):
        self.confirm_publish = confirm_publish

    def get_exchange(self, exchange_name):
        return Exchange(exchange_name, type='topic', auto_delete=True)

    @property
    def amqp_uri(self):
        return self.config['AMQP_URI']

    @property
    def transport_options(self):
        return {
            'confirm_publish': self.confirm_publish
        }

    async def setup(self):
        self._publisher = Publisher(self.amqp_uri)

    async def get_dependency(self, worker):
        self.config = worker.config

        def dispatch_event(service_name, event_name, payload):
            exchange_name = None
            if service_name is not None:
                exchange_name = f"{service_name}.events"

            exchange = self.get_exchange(exchange_name)
            try:
                return self._publisher.publish(
                    payload, exchange=exchange, routing_key=event_name
                )
            except NotFound:
                pass

        return dispatch_event

    def __call__(self, service_name: str, event_name: str, payload: Union[str, Dict]):
        ... # Signature of the injected dependency


class Producer(SystemProducer):
    def __init__(self, confirm_publish=True, service_name=None):
        self.service_name = service_name
        super(Producer, self).__init__(confirm_publish)

    async def get_dependency(self, worker):
        self.config = worker.config
        service_name = self.service_name or self.config['SERVICE_NAME']
        exchange_name = f"{service_name}.events"  # Nameko Compatible
        exchange = self.get_exchange(exchange_name)

        def dispatch_event(event_name, payload):
            try:
                return self._publisher.publish(
                    payload, exchange=exchange, routing_key=event_name
                )
            except NotFound:
                pass

        return dispatch_event

    def __call__(self, event_name: str, payload: Union[str, Dict]):
        ... # Signature of the injected dependency


class AMQPRPCFactories(RPCProviderFactories):
    def __init__(self, provider, config):
        super(AMQPRPCFactories, self).__init__(provider, config)
        self.reply_listener = provider.reply_listener.picklable_listener

    def new_connector(self, service_name: str) -> AMQPRPCConnector:
        return AMQPRPCConnector(
            self.config.get("AMQP_URI"),
            target_service=service_name,
            reply_listener=self.reply_listener
        )


class RPCProxyProvider(RPCServiceProvider.with_factory(AMQPRPCFactories)):
    """
    Provides a RPCServiceProxy with AMQPRPCConnector class handling the
    communication through AMQP Protocol.
    """
    reply_listener = RPCReplyListener()


class RPCSystemProxyProvider(RPCProvider.with_factory(AMQPRPCFactories)):
    """
    Provides RPCProxy instance with AMQPRPCConnector class handling the
    communication through AMQP Protocol.
    """
    reply_listener = RPCReplyListener()


def dispatch(amqp_uri, exchange, routing_key, payload, amqp_heartbeat=None,
             **kwargs):
    """
    Helper to dispatch a single payload that will load the client and call
    the send method.

    :param str amqp_uri:
    :param Exchange exchange:
    :param str routing_key:
    :param Any payload:
    :param dict headers:
    """
    connection = get_connection(amqp_uri, heartbeat=amqp_heartbeat)
    with producers[connection].acquire(block=True) as producer:
        return producer.publish(
            payload, exchange=exchange, routing_key=routing_key,
            **kwargs
        )
