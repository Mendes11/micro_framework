import logging
from functools import partial
from typing import Dict

from kombu import Exchange, producers

from micro_framework.amqp.amqp_elements import get_connection, Publisher
from micro_framework.amqp.connectors import AMQPRPCConnector
from micro_framework.amqp.rpc import RPCReplyListener
from micro_framework.dependencies import Dependency, RunnerDependency
from micro_framework.rpc import RPCDependencyMixin, RPCServiceProxy, RPCProxy

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


class AMQPServiceProxy(RPCServiceProxy):
    def __init__(self, amqp_uri, reply_listener, service_name):
        self.amqp_uri = amqp_uri
        self.reply_listener = reply_listener
        super(AMQPServiceProxy, self).__init__(service_name)

    connector_class = AMQPRPCConnector

    def get_connector_kwargs(self) -> Dict:
        return {
            "amqp_uri": self.amqp_uri,
            "target_service": self.name,
            "reply_listener": self.reply_listener,
        }


class RPCServiceProxyProvider(RPCDependencyMixin, RunnerDependency):
    """
    Provides a RPCServiceProxy with AMQPRPCConnector already configured to
    the target_service.
    """

    def __init__(self, target_service):
        self.target_service = target_service

    proxy_class = AMQPServiceProxy
    reply_listener = RPCReplyListener()

    async def get_proxy_kws(self):
        kw = await super(RPCServiceProxyProvider, self).get_proxy_kws()
        kw["reply_listener"] = self.reply_listener.picklable_listener
        kw["amqp_uri"] = self.config["AMQP_URI"]
        kw["service_name"] = self.target_service
        return kw

    async def get_proxy(self):
        cls = self.get_proxy_class()
        kws = await self.get_proxy_kws()
        partial_call = partial(cls, **kws)
        return await self.runner.event_loop.run_in_executor(
            None, partial_call
        )


class AMQPProxy(RPCProxy):
    def __init__(self, amqp_uri, reply_listener):
        self.amqp_uri = amqp_uri
        self.reply_listener = reply_listener
        super(AMQPProxy, self).__init__()

    service_proxy_class = AMQPServiceProxy

    def get_service_proxy_kwargs(self, service_name):
        kw = super(AMQPProxy, self).get_service_proxy_kwargs(service_name)
        kw["amqp_uri"] = self.amqp_uri
        kw["reply_listener"] = self.reply_listener
        kw["service_name"] = service_name
        return kw


class RPCProxyProvider(RPCDependencyMixin, RunnerDependency):
    proxy_class = AMQPProxy
    reply_listener = RPCReplyListener()

    async def get_proxy_kws(self):
        kw = await super(RPCProxyProvider, self).get_proxy_kws()
        kw["reply_listener"] = self.reply_listener.picklable_listener
        kw["amqp_uri"] = self.config["AMQP_URI"]
        return kw


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
