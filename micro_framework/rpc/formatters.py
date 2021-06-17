import importlib
import inspect
import json

import micro_framework
from micro_framework.exceptions import RPCException


def import_exception(exception):
    fw_exceptions = dict(inspect.getmembers(
        micro_framework.exceptions,
        lambda x: inspect.isclass(x) and issubclass(x, Exception)
    ))

    args = exception.get("args", (exception.get("message"), ))
    kwargs = exception.get("kwargs", {})

    if exception['exception'] in __builtins__:
        return __builtins__[exception['exception']](*args, **kwargs)
    elif exception["exception"] in fw_exceptions:
        return fw_exceptions[exception["exception"]](*args, **kwargs)

    try:
        module = importlib.import_module(exception["module"])
        return getattr(module, exception["exception"])(*args, **kwargs)
    except Exception:
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
        args = exception.__getargs__() if hasattr(exception, "__getargs__") else exception.args
        kwargs = exception.__getkwargs__() if hasattr(exception, "__getkwargs__") else {}
        if kwargs is None: kwargs = {}
        try:
            module = exception.__module__
        except:
            module = None

        exception_data = {
            'exception': type(exception).__name__,
            'message': str(exception),
            'args': args,
            "kwargs": kwargs,
            'module': module,
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