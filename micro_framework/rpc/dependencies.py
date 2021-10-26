from typing import Type, Callable

from micro_framework import Worker
from micro_framework.dependencies import RunnerDependency
from micro_framework.rpc.connectors import RPCConnector
from micro_framework.rpc.proxies import RPCService, RPCSystem, RPCTarget
from micro_framework.rpc.client import RPCClient


class RPCProviderMixin:
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


class RPCServiceProvider(RPCProviderMixin, RunnerDependency):
    def __init__(self, service_name: str):
        self.service_name = service_name

    async def get_dependency(self, worker: Worker) -> Callable:
        return self.new_service_proxy(self.service_name)


class RPCProvider(RPCProviderMixin, RunnerDependency):
    async def get_dependency(self, worker: Worker) -> Callable:
        return self.get_rpc_system_proxy()
