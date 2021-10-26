from concurrent.futures import Future

import pytest
from mock import call

from micro_framework.rpc.proxies import RPCSystem, RPCService, RPCTarget


@pytest.fixture
def service_factory(mocker):
    return mocker.MagicMock()

@pytest.fixture
def rpc_service(mocker):
    return mocker.MagicMock()

@pytest.fixture
def rpc_target(mocker):
    return mocker.MagicMock(target_id="my_target")


@pytest.fixture
def target_factory(mocker, rpc_target):
    return mocker.MagicMock(return_value=rpc_target)


@pytest.fixture
def client(mocker):
    return mocker.MagicMock()



def test_rpc_system_service_proxy_created(service_factory):
    rpc = RPCSystem(service_factory)

    assert rpc.some_service == service_factory.return_value


def test_rpc_system_service_proxy_factory_called(service_factory):
    rpc = RPCSystem(service_factory)
    _ = rpc.some_service
    service_factory.assert_called_with("some_service")


def test_rpc_system_service_proxy_multiple_calls(service_factory):
    service_factory.side_effect = ["test", "test2"]
    rpc = RPCSystem(service_factory)
    s1 = rpc.some_service
    s2 = rpc.some_service
    assert s1 == s2 == "test"
    service_factory.assert_called_once()


def test_rpc_service_target_created(target_factory, client, rpc_target):
    service = RPCService("service_name", target_factory, client)
    rpc_target.target_id = "my_target"

    assert service.my_target == rpc_target


def test_rpc_service_target_proxy_factory_called(
        target_factory, client, rpc_target
):
    service = RPCService("service_name", target_factory, client)
    rpc_target.target_id = "my_target"
    _ = service.my_target

    target_factory.assert_called_with("my_target", service, None, None)


def test_rpc_service_target_proxy_multiple_calls(
        target_factory, client, rpc_target
):
    service = RPCService("service_name", target_factory, client)
    rpc_target.target_id = "my_target"
    s1 = service.my_target
    s2 = service.my_target
    assert s1 == s2 == rpc_target
    target_factory.assert_called_once()


def test_rpc_service_target_discovery(
    target_factory, client
):
    targets = [
        {"target": "my_func", "args": ("arg1", "arg2")},
        {"target": "MyClass.func2", "args": ("arg1", ), "kwargs": ("arg2", )}
    ]
    service = RPCService("service_name", target_factory, client)
    client.list_targets.return_value = targets
    service.discover_targets()

    client.list_targets.assert_called_once()
    target_factory.assert_has_calls([
        call("my_func", service, targets[0], None),
        call("MyClass.func2", service, targets[1], None),
        ]
    )

def test_rpc_service_target_discovery_attrs_set(
    target_factory, client, mocker
):
    targets = [
        {"target": "my_func", "args": ("arg1", "arg2")},
        {"target": "MyClass.func2", "args": ("arg1", ), "kwargs": ("arg2", )}
    ]
    service = RPCService("service_name", target_factory, client)
    client.list_targets.return_value = targets
    targets = [
        mocker.MagicMock(target_id="my_func"),
        mocker.MagicMock(target_id="MyClass"),
    ]
    target_factory.side_effect = targets
    service.discover_targets()

    assert hasattr(service, "my_func")
    assert hasattr(service, "MyClass")
    assert service.my_func == targets[0]
    assert service.MyClass == targets[1]
    assert service.targets == set(targets)


def test_rpc_target_instantiated_not_nested(rpc_service):
    target = RPCTarget("my_func", rpc_service)
    assert target.target_id == "my_func"
    assert target._service == rpc_service


def test_rpc_target_instantiated_nested(rpc_service, mocker):
    rpc_service._new_rpc_target.side_effect = (
        lambda target_id, details, parent: RPCTarget(
            target_id, rpc_service, details, parent=parent
        )
    )
    target = RPCTarget("MyClass.my_func", rpc_service)

    assert target.target_id == "MyClass"
    assert hasattr(target, "my_func")
    assert target.my_func.target_id == "MyClass.my_func"
    rpc_service._new_rpc_target.assert_called_with(
        "my_func", None, parent=target
    )

def test_rpc_target_instantiated_nested_two_lvls(rpc_service, mocker):
    rpc_service._new_rpc_target.return_value = mocker.MagicMock(
        _target_id="my_func"
    )
    target = RPCTarget("MyClass.my_func.something", rpc_service)

    assert target.target_id == "MyClass"
    assert hasattr(target, "my_func")
    rpc_service._new_rpc_target.assert_called_with(
        "my_func.something", None, parent=target
    )

def test_rpc_target_unknown_levels(rpc_service, mocker):
    t1 = mocker.MagicMock(target_id="my_func")
    rpc_service._new_rpc_target.return_value = t1
    target = RPCTarget("MyClass", rpc_service)

    assert target.target_id == "MyClass"

    inner_target = target.my_func
    assert inner_target == t1

    rpc_service._new_rpc_target.assert_has_calls([
        call("my_func", None, parent=target),
    ])


def test_rpc_target_nested_levels(rpc_service, mocker):
    rpc_service._new_rpc_target.side_effect = (
        lambda target_id, details, parent: RPCTarget(
            target_id, rpc_service, details, parent=parent
        )
    )

    target = RPCTarget("MyClass", rpc_service)

    assert target.target_id == "MyClass"

    inner_target = target.my_func
    assert isinstance(inner_target, RPCTarget)

    inner_target2 = inner_target.another_func
    assert isinstance(inner_target2, RPCTarget)

    rpc_service._new_rpc_target.assert_has_calls([
        call("my_func", None, parent=target),
        call("another_func", None, parent=inner_target)
    ])
    assert inner_target.target_id == "MyClass.my_func"
    assert inner_target2.target_id == "MyClass.my_func.another_func"

def test_rpc_target_with_details(rpc_service):
    details = {
        "target": "my_func", "args": ("arg1", "arg2"),
        "kwargs": ("kwarg1", ), "docstring": "some long text"
    }
    target = RPCTarget("my_func", rpc_service, target_details=details)

    assert target.target_id == "my_func"
    assert target.target_args == ("arg1", "arg2")
    assert target.target_kwargs == ("kwarg1", )
    assert target.target_doc == "some long text"

    rpc_service._new_rpc_target.assert_not_called()


def test_rpc_target_with_details_nested(rpc_service, mocker):
    rpc_service._new_rpc_target.side_effect = (
        lambda target_id, details, parent: RPCTarget(
            target_id, rpc_service, details, parent=parent
        )
    )
    details = {
        "target": "MyClass.my_func", "args": ("arg1", "arg2"),
        "kwargs": ("kwarg1",), "docstring": "some long text"
    }
    target = RPCTarget(
        "MyClass.my_func", rpc_service, target_details=details
    )
    assert target.target_id == "MyClass"
    assert target.target_args == None
    assert target.target_kwargs == None
    assert target.target_doc == None

    rpc_service._new_rpc_target.assert_called_with(
        "my_func", details, parent=target
    )


def test_rpc_target_call(rpc_service, client):
    rpc_service._client = client
    target = RPCTarget("my_func", rpc_service)
    future = Future()
    future.set_result("result")

    client.call_target_async.return_value = future

    t = target(1, 2, 3, 4, test="something")
    assert t == "result"
    client.call_target_async.assert_called_with(
        "my_func", 1, 2, 3, 4, test="something", _wait_response=True
    )

def test_rpc_target_call_async(rpc_service, client):
    rpc_service._client = client

    target = RPCTarget("my_func", rpc_service)
    future = Future()
    future.set_result("result")

    client.call_target_async.return_value = future

    t = target.call_async(1, 2, 3, 4, test="something")
    client.call_target_async.assert_called_with(
        "my_func", 1, 2, 3, 4, test="something",  _wait_response=True
    )
    assert t == future
    assert t.result() == "result"
