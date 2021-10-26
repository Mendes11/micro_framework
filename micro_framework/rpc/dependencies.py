from typing import Type, Callable

from micro_framework import Worker
from micro_framework.dependencies import RunnerDependency
from micro_framework.rpc.connectors import RPCConnector
from micro_framework.rpc.proxies import RPCService, RPCSystem, RPCTarget
from micro_framework.rpc.client import RPCClient


class RPCProviderFactories:
    """
    Class with all RPC factories.

    Ideally, this class should be subclassed by the providers, but since we
    are working with factories, which consists of sending to the classes the
    constructors (factories) of RPC Objects, we end up sending a reference
    that, when pickling for process spawners, has pickling problems due to
    the reference of the factory being the RunnerDependency and therefore we
    end up having to pickle it, which raises an error

    The solution was to have a separate class that handles all factories and
    to ensure it won't store any reference of the provider
    """
    def __init__(self, provider, config):
        # NOTE: do not store the provider class, since it is not pickleable!
        self.config = config

    def new_connector(self, service_name: str) -> RPCConnector:
        return RPCConnector()

    def new_client(self, service_name: str) -> RPCClient:
        return RPCClient(self.new_connector(service_name))

    def new_rpc_target_proxy(
            self, target_id: str, rpc_service, target_details, parent
    ) -> RPCTarget:
        return RPCTarget(
            target_id,
            rpc_service,
            target_details=target_details,
            parent=parent
        )

    def new_service_proxy(self, service_name: str) -> Type[RPCService]:
        return RPCService(
            service_name,
            rpc_target_factory=self.new_rpc_target_proxy,
            client=self.new_client(service_name)
        )

    def get_rpc_system_proxy(self):
        return RPCSystem(
            service_proxy_factory=self.new_service_proxy
        )


class RPCProvider(RunnerDependency):
    """
    Provides a RPCSystem class that enables the communication with
    micro-framework's RPC Manager from any running service.

    To use this class you must at least implement the connector logic in a
    factory class that subclasses RPCProviderFactories and then create a new
    RPCProvider class subclassing this one by calling `with_factory`
    class method.
    ex:
    class MyRPCProviderFactories(RPCProviderFactories):
        def new_connector(self, service_name: str):
            return MyConnector()


    class MyRPCProvider(RPCProvider.with_factory(MyRPCProviderFactories)):
        ...

    """
    factory_class = RPCProviderFactories

    async def get_factories(self, worker: Worker):
        return self.factory_class(self, worker.config)

    async def setup_dependency(self, worker: Worker):
        self.factories = await self.get_factories(worker)

    async def get_dependency(self, worker: Worker) -> Callable:
        return self.factories.get_rpc_system_proxy()

    @classmethod
    def with_factory(cls, factory_class):
        name = "{} with {}".format(cls.__name__, factory_class.__name__)
        return type(name, (cls, ), {"factory_class": factory_class})


class RPCServiceProvider(RPCProvider):
    def __init__(self, service_name: str):
        self.service_name = service_name

    async def get_dependency(self, worker: Worker) -> Callable:
        return self.factories.new_service_proxy(self.service_name)
