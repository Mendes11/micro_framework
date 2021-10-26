from concurrent.futures import Future

import pytest

from micro_framework.rpc.client import RPCClient


@pytest.fixture
def connection(mocker):
    return mocker.MagicMock()


@pytest.fixture
def connector(mocker, connection):
    c = mocker.MagicMock()
    c.get_connection.return_value.__enter__.return_value = connection
    return c


def test_rpc_client_make_call(connector, connection):
    cli = RPCClient(connector)
    connection.send_and_receive.return_value = '{"data": "some_value"}'

    res = cli.make_call({"some_message": "here"})
    assert res == "some_value"
    connection.send_and_receive.assert_called_with('{"some_message": "here"}')


def test_rpc_client_make_call_no_wait(connector, connection):
    cli = RPCClient(connector)

    res = cli.make_call({"some_message": "here"}, wait_response=False)
    assert res is None
    connection.send_and_receive.assert_not_called()
    connection.send.assert_called_with('{"some_message": "here"}')


def test_rpc_client_make_call_exception_returned(connector, connection):
    cli = RPCClient(connector)
    connection.send_and_receive.return_value = '{"exception": {"exception": ' \
                                               '"ValueError"}}'
    res = cli.make_call({"some_message": "here"})

    assert isinstance(res, ValueError)


def test_rpc_client_make_async_call(connector, connection):
    cli = RPCClient(connector)
    connection.send_and_receive.return_value = '{"data": "some_value"}'

    res = cli.make_async_call({"some_message": "here"})
    assert isinstance(res, Future)
    assert res.result() == "some_value"
    connection.send_and_receive.assert_called_with('{"some_message": "here"}')


def test_rpc_client_make_async_call_no_wait(connector, connection):
    cli = RPCClient(connector)
    res = cli.make_async_call({"some_message": "here"}, wait_response=False)
    assert isinstance(res, Future)
    assert res.result() is None

    connection.send_and_receive.assert_not_called()
    connection.send.assert_called_with('{"some_message": "here"}')


def test_rpc_client_list_targets(connector, connection):
    cli = RPCClient(connector)
    connection.send_and_receive.return_value = '{"data": ["test"]}'

    res = cli.list_targets()
    assert res == ["test"]


def test_rpc_client_list_targets_command_sent(connector, connection):
    cli = RPCClient(connector)
    connection.send_and_receive.return_value = '{"data": ["test"]}'
    cli.list_targets()

    connector.get_connection.assert_called_once()
    connection.send_and_receive.assert_called_with(
        '{"command": "__list_targets__", "command_args": [], '
        '"command_kwargs": {}}'
    )


def test_rpc_client_list_targets_async(connector, connection):
    cli = RPCClient(connector)
    connection.send_and_receive.return_value = '{"data": ["test"]}'

    fut = cli.list_targets_async()
    assert isinstance(fut, Future)

    assert fut.result() == ["test"]


def test_rpc_client_get_target(connector, connection):
    cli = RPCClient(connector)
    connection.send_and_receive.return_value = '{"data": {"target": "test"}}'

    res = cli.get_target("test")
    assert res == {"target": "test"}

def test_rpc_client_get_target_async(connector, connection):
    cli = RPCClient(connector)
    connection.send_and_receive.return_value = '{"data": {"target": "test"}}'

    fut = cli.get_target_async("test")
    assert isinstance(fut, Future)

    assert fut.result() == {"target": "test"}


def test_rpc_client_get_target_command(connector, connection):
    cli = RPCClient(connector)
    connection.send_and_receive.return_value = '{"data": {"target": "test"}}'
    cli.get_target("test")

    connection.send_and_receive.assert_called_with(
        '{"command": "__get_target__", "command_args": ["test"], '
        '"command_kwargs": {}}'
    )

def test_rpc_client_call_target(connector, connection):
    cli = RPCClient(connector)
    connection.send_and_receive.return_value = '{"data": "result"}'

    res = cli.call_target("test", "arg1", 2, kwarg1="test")

    assert res == "result"


def test_rpc_client_call_target_command(connector, connection):
    cli = RPCClient(connector)
    connection.send_and_receive.return_value = '{"data": "result"}'

    cli.call_target("test", "arg1", 2, kwarg1="test_kwarg")
    connection.send_and_receive.assert_called_with(
        '{"command": "test", "command_args": ["arg1", 2], '
        '"command_kwargs": {"kwarg1": "test_kwarg"}}'
    )


def test_rpc_client_call_target_async(connector, connection):
    cli = RPCClient(connector)
    connection.send_and_receive.return_value = '{"data": "result"}'

    res = cli.call_target_async("test", "arg1", 2, kwarg1="test")
    assert isinstance(res, Future)
    assert res.result() == "result"
