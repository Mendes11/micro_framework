import pytest
from mock import AsyncMock

from micro_framework.rpc.manager import RPCRegister, RPCManager

pytestmark = pytest.mark.asyncio


@pytest.fixture
def rpc_register():
    return RPCRegister()


@pytest.fixture
async def rpc_manager():
    manager = RPCManager()
    await manager.register.clear()
    return manager


@pytest.fixture
def entrypoint():
    e = AsyncMock()
    e.route.target.target_detail.return_value = {
        "target": "target_id", "args": ("something",)
    }
    return e


async def test_rpc_register_add_entrypoint(rpc_register, entrypoint):
    await rpc_register.add_entrypoint(entrypoint)

    assert rpc_register.targets == {
        "target_id": entrypoint
    }


async def test_rpc_register_remove_entrypoint(rpc_register, entrypoint):
    await rpc_register.add_entrypoint(entrypoint)
    assert rpc_register.targets == {
        "target_id": entrypoint
    }

    await rpc_register.remove_entrypoint(entrypoint)
    assert rpc_register.targets == {}


async def test_rpc_register_entrypoints(rpc_register, entrypoint):
    await rpc_register.add_entrypoint(entrypoint)
    assert rpc_register.entrypoints == [entrypoint]

async def test_rpc_register_clear(rpc_register, entrypoint):
    await rpc_register.add_entrypoint(entrypoint)
    await rpc_register.clear()
    assert rpc_register.targets == {}


async def test_rpc_manager_add_entrypoint(rpc_manager, entrypoint):
    await rpc_manager.add_entrypoint(entrypoint)

    assert rpc_manager.register.targets == {"target_id": entrypoint}
    assert rpc_manager.entrypoints == {"target_id": entrypoint}


async def test_rpc_manager_add_entrypoint_already_registered(
        rpc_manager, entrypoint
):
    new_entrypoint = AsyncMock()
    new_entrypoint.route.target.target_detail.return_value = {
        "target": "target_id", "args": ("something",)
    }
    await rpc_manager.add_entrypoint(entrypoint)

    # Now we try to add another entrypoint that has the same target being
    # exposed. Since we reference through the target key, this should not be
    # allowed in the **same manager**.
    with pytest.raises(ValueError) as exc:
        await rpc_manager.add_entrypoint(new_entrypoint)
        assert exc == (
            f"Entrypoint {entrypoint} is already registered. "
            f"You cannot have multiple entrypoints to the same target"
        )


async def test_rpc_manager_consume_list_targets_message(
        rpc_manager, entrypoint, mocker
):
    await rpc_manager.add_entrypoint(entrypoint)
    client = mocker.MagicMock()

    message = '{"command": "__list_targets__"}'

    res = await rpc_manager.consume_message(client, message)

    assert res == '{"data": [{"target": "target_id", "args": ["something"]}], "exception": null}'


async def test_rpc_manager_consume_message_as_dict(
        rpc_manager, entrypoint, mocker
):
    await rpc_manager.add_entrypoint(entrypoint)
    client = mocker.MagicMock()

    message = {"command": "__list_targets__"}

    res = await rpc_manager.consume_message(client, message)

    assert res == '{"data": [{"target": "target_id", "args": ["something"]}], "exception": null}'


async def test_rpc_manager_consume_message_format_error(
        rpc_manager, entrypoint, mocker
):
    await rpc_manager.add_entrypoint(entrypoint)
    client = mocker.MagicMock()

    message = "not a json message"

    res = await rpc_manager.consume_message(client, message)
    err_msg = "RPCManager received unknown message structure: not a json message"
    assert res == (
        '{"data": null, '
        '"exception": {"exception": "RPCMalformedMessage", '
        f'"message": "{err_msg}", "args": ["{err_msg}"], '
        '"kwargs": {}, "module": "micro_framework.exceptions"}}'
    )


async def test_rpc_manager_consume_get_target_message(
        rpc_manager, entrypoint, mocker
):
    await rpc_manager.add_entrypoint(entrypoint)
    client = mocker.MagicMock()

    message = '{"command": "__get_target__", "command_args": ["target_id"]}'

    res = await rpc_manager.consume_message(client, message)

    assert res == '{"data": {"target": "target_id", "args": ["something"]}, "exception": null}'


async def test_rpc_manager_consume_get_target_message_target_not_found(
        rpc_manager, mocker
):
    client = mocker.MagicMock()

    message = '{"command": "__get_target__", "command_args": ["target_id"]}'

    res = await rpc_manager.consume_message(client, message)

    assert res == '{"data": null, "exception": {"exception": "KeyError", ' \
                  '"message": "\'target_id\'", "args": ["target_id"], ' \
                  '"kwargs": {}, "module": null}}'


async def test_rpc_manager_call_target(rpc_manager, mocker, entrypoint):
    client = mocker.MagicMock()
    worker = mocker.MagicMock(result="route_response", exception=None)

    await rpc_manager.add_entrypoint(entrypoint)
    entrypoint.call_route.return_value = worker

    message = '{"command": "target_id", "command_args": ["test"]}'
    res = await rpc_manager.consume_message(client, message)

    assert res == '{"data": "route_response", "exception": null}'


async def test_rpc_manager_call_target_exception_raised(
        rpc_manager, mocker, entrypoint
):
    client = mocker.MagicMock()
    worker = mocker.MagicMock(result=None, exception=ValueError("test"))

    await rpc_manager.add_entrypoint(entrypoint)
    entrypoint.call_route.return_value = worker

    message = '{"command": "target_id", "command_args": ["test"]}'
    res = await rpc_manager.consume_message(client, message)

    assert res == '{"data": null, "exception": {"exception": "ValueError", ' \
                  '"message": "test", "args": ["test"], ' \
                  '"kwargs": {}, "module": null}}'

async def test_rpc_manager_call_target_not_found(
        rpc_manager, mocker
):
    client = mocker.MagicMock()

    message = '{"command": "target_id", "command_args": ["test"]}'
    res = await rpc_manager.consume_message(client, message)

    err_msg = "target_id does not exist or is not exposed through a RPC method."
    assert res == (
        '{"data": null, "exception": {"exception": "RPCTargetDoesNotExist", '
        f'"message": "{err_msg}", '
        f'"args": ["{err_msg}"], '
        '"kwargs": {}, "module": "micro_framework.exceptions"}}'
    )
