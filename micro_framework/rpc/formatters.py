import inspect
import json

import micro_framework
from micro_framework.exceptions import RPCException


def import_exception(exception):
    fw_exceptions = dict(inspect.getmembers(
        micro_framework.exceptions,
        lambda x: inspect.isclass(x) and issubclass(x, Exception)
    ))
    if exception['exception'] in __builtins__:
        return __builtins__[exception['exception']](exception['message'])
    elif exception["exception"] in fw_exceptions:
        return fw_exceptions[exception["exception"]](exception.get("message"))
    return RPCException(exception)


def format_rpc_command(command, *args, **kwargs):
    """
    Returns a json str with the expected structure to communicate with our
    RPCManager.

    :param str command: RPC Command. Can be either list/get targets or a call
    to the target by its name.
    :param args: Arguments to be sent to the RPC Command.
    :param kwargs: Key Arguments to be sent to the RPC Command.
    :return str: JSON Command
    """
    return json.dumps({
        'command': command, 'command_args': args, 'command_kwargs': kwargs
    })


def format_rpc_response(data, exception=None):
    """
    Formats a response from a RPC Manager.
    It provides the data and/or a serialized exception so it can be
    re-created by the caller.

    :param Any data: A JSON Serializable object.
    :param Exception exception: An Exception object
    :return str: JSON Response.
    """
    exception_data = None
    if exception:
        exception_data = {
            'exception': type(exception).__name__,
            'message': str(exception),
        }
    return json.dumps({
        'data': data,
        'exception': exception_data
    })


def parse_rpc_response(message):
    """
    Reads a response returned by the RPC Manager.
    It returns either a loaded JSON or an exception if the exception key is
    not None.

    :param str message: JSON Response
    :return dict|list|Exception: Loaded RPC Response.
    """
    response = json.loads(message)
    if response.get('exception'):
        return import_exception(response['exception'])
    return response.get('data')