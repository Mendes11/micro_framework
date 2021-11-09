import pytest

from micro_framework.rpc import RPCProvider, RPCServiceProvider
from micro_framework.rpc.proxies import RPCSystem, RPCService

pytestmark = pytest.mark.asyncio

@pytest.fixture
def worker(mocker):
    return mocker.MagicMock(config={})


async def test_rpc_provider(worker, mocker):
    rpc_factory = mocker.MagicMock(__name__="MockFactory")
    provider = RPCProvider.with_factory(rpc_factory)()
    await provider.setup_dependency(worker)
    dep = await provider.get_dependency(worker)

    assert dep == rpc_factory().get_rpc_system_proxy.return_value

async def test_rpc_service_provider(mocker, worker):
    rpc_factory = mocker.MagicMock(__name__="MockFactory", spec=RPCService)
    provider = RPCServiceProvider.with_factory(rpc_factory)("test_service")

    await provider.setup_dependency(worker)
    dep = await provider.get_dependency(worker)

    assert dep == rpc_factory().new_service_proxy.return_value
