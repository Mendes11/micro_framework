import logging
from functools import partial
from typing import Dict

from kombu import Exchange, producers

from micro_framework.amqp.amqp_elements import get_connection
from micro_framework.amqp.connectors import AMQPRPCConnector
from micro_framework.amqp.rpc import RPCReplyListener
from micro_framework.dependencies import Dependency, RunnerDependency
from micro_framework.rpc import RPCDependency

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
    
    async def get_dependency(self, worker):
        exchange = self.exchange
        def dispatch_event(event_name, payload):
            return dispatch(self.amqp_uri, exchange, event_name, payload)
        return dispatch_event


class RPCProxyProvider(RPCDependency):
    """
    Provides a RPCProxy with AMQPRPCConnector.
    """

    def __init__(self, target_service):
        self.target_service = target_service
        super(RPCProxyProvider, self).__init__()

    reply_listener = RPCReplyListener()
    connector_class = AMQPRPCConnector

    def get_connector_kwargs(self) -> Dict:
        return {
            "amqp_uri": self.config.get("AMQP_URI"),
            "target_service": self.target_service,
            "reply_listener": self.reply_listener.picklable_listener,
        }


def dispatch(amqp_uri, exchange, routing_key, payload, **kwargs):
    """
    Helper to dispatch a single payload that will load the client and call
    the send method.

    :param str amqp_uri:
    :param Exchange exchange:
    :param str routing_key:
    :param Any payload:
    :param dict headers:
    """
    connection = get_connection(amqp_uri)
    with producers[connection].acquire(block=True) as producer:
        return producer.publish(
            payload, exchange=exchange, routing_key=routing_key,
            **kwargs
        )
