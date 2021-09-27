import pytest

from micro_framework.rpc import RPCProvider, RPCServiceProvider
from micro_framework.rpc.proxies import RPCSystem, RPCService

pytestmark = pytest.mark.asyncio


async def test_rpc_provider():
    provider = RPCProvider()

    dep = await provider.get_dependency(None)

    assert isinstance(dep, RPCSystem)
    assert dep.new_service_proxy == provider.new_service_proxy


async def test_rpc_service_provider(mocker):
    provider = RPCServiceProvider("test_service")

    connector = mocker.patch("micro_framework.rpc.dependencies.RPCConnector")
    client = mocker.patch("micro_framework.rpc.dependencies.RPCClient")

    dep = await provider.get_dependency(None)

    assert isinstance(dep, RPCService)
    assert dep._target_factory == provider.new_rpc_target_proxy
    assert dep._client == provider.new_client("test_service")

    client.assert_called_with(connector.return_value)
