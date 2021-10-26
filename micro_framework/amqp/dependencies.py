import logging
from functools import partial
from typing import Dict

from kombu import Exchange, producers

from micro_framework.amqp.amqp_elements import get_connection, Publisher
from micro_framework.amqp.connectors import AMQPRPCConnector
from micro_framework.amqp.rpc import RPCReplyListener
from micro_framework.dependencies import Dependency, RunnerDependency
from micro_framework.rpc import RPCServiceProvider, RPCProvider

logger = logging.getLogger(__name__)


class Producer(Dependency):
    def __init__(self, confirm_publish=True, service_name=None):
        self.service_name = service_name
        self.confirm_publish = confirm_publish

    @property
    def exchange(self):
        service_name = self.service_name or self.config['SERVICE_NAME']
        exchange_name = f"{service_name}.events"  # Nameko Compatible
        return Exchange(exchange_name, type='topic', auto_delete=True)

    @property
    def amqp_uri(self):
        return self.config['AMQP_URI']

    @property
    def transport_options(self):
        return {
            'confirm_publish': self.confirm_publish
        }
    
    def bind(self, runner, parent=None):
        return super(Producer, self).bind(runner, parent)

    async def setup(self):
        self._publisher = Publisher(self.amqp_uri)

    async def get_dependency(self, worker):
        self.config = worker.config
        exchange = self.exchange

        def dispatch_event(event_name, payload):
            return self._publisher.publish(
                payload, exchange=exchange, routing_key=event_name
            )

        return dispatch_event


class RPCProviderMixin:
    reply_listener = RPCReplyListener()

    def new_connector(self, service_name: str) -> AMQPRPCConnector:
        return AMQPRPCConnector(
            self.config.get("AMQP_URI"),
            target_service=service_name,
            reply_listener=self.reply_listener.picklable_listener
        )


class RPCProxyProvider(RPCProviderMixin, RPCServiceProvider):
    """
    Provides a RPCServiceProxy with AMQPRPCConnector class handling the
    communication through AMQP Protocol.
    """


class RPCSystemProxyProvider(RPCProviderMixin, RPCProvider):
    """
    Provides RPCProxy instance with AMQPRPCConnector class handling the
    communication through AMQP Protocol.
    """

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
