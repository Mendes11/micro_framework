from micro_framework.rpc.formatters import import_exception, \
    format_rpc_response, parse_rpc_response
from micro_framework.exceptions import MaxConnectionsReached, RPCException

class MockException(Exception):
    pass


class MockExceptionWithArgs(Exception):
    def __init__(self, message, arg2=None):
        self.arg2 = arg2
        super(MockExceptionWithArgs, self).__init__(message)


class MockExceptionWithGetKwargs(Exception):
    def __init__(self, message, arg1, arg2=None):
        self.arg1 = arg1
        self.arg2 = arg2
        super(MockExceptionWithGetKwargs, self).__init__(message)

    def __getkwargs__(self):
        return {"arg2": self.arg2}

    def __getargs__(self):
        return (self.args[0], self.arg1)


def test_import_exception_builtin():
    exc = import_exception({
        "exception": "ValueError",
        "message": "Wrong Value"
    })
    assert type(exc) == ValueError
    assert str(exc) == "Wrong Value"


def test_import_exception_framework_exc():
    exc = import_exception({
        "exception": "MaxConnectionsReached",
        "message": "ErrorMessage",
    })
    assert type(exc) == MaxConnectionsReached
    assert str(exc) == "ErrorMessage"


def test_import_exception_custom():
    exc = import_exception({
        "exception": "MockException",
        "message": "Some Message",
        "module": MockException.__module__
    })
    assert type(exc) == MockException
    assert str(exc) == "Some Message"


def test_import_exception_custom_no_args():
    exc = import_exception({
        "exception": "MockExceptionWithArgs",
        "message": "Some Message",
        "module": MockException.__module__,
    })
    assert type(exc) == MockExceptionWithArgs
    assert exc.args == ("Some Message", )
    assert exc.arg2 is None
    assert str(exc) == "Some Message"

def test_import_exception_with_args():
    exc = import_exception({
        "exception": "MockExceptionWithArgs",
        "message": "Test",
        "module": MockException.__module__,
        "args": ["Test"]
    })
    assert type(exc) == MockExceptionWithArgs
    assert exc.args == ("Test", )
    assert exc.arg2 == None
    assert str(exc) == "Test"


def test_import_exception_with_kwargs():
    exc = import_exception({
        "exception": "MockExceptionWithArgs",
        "message": "Test",
        "module": MockException.__module__,
        "args": ["Test", ],
        "kwargs": {"arg2": "Test2"}
    })
    assert type(exc) == MockExceptionWithArgs
    assert exc.args == ("Test", )
    assert exc.arg2 == "Test2"
    assert str(exc) == "Test"


def test_import_exception_no_module_found():
    exc = import_exception({
        "exception": "ServerSpecificException",
        "message": "Some Message",
        "module": "inexistent.module",
        "args": ["Not Used", ]
    })
    assert type(exc) == RPCException
    assert str(exc) == "ServerSpecificException Some Message"


def test_format_rpc_response_normal_data():
    data = {"response": "this is a response"}
    ret = format_rpc_response(data)
    assert ret == ('{'
        '"data": {'
            '"response": "this is a response"'
        '}, '
        '"exception": null'
   '}')


def test_format_rpc_response_with_exception_no_getkwargs():
    exc = MockExceptionWithArgs("test", arg2="test2")
    ret = format_rpc_response(None, exception=exc)
    assert ret == ('{'
      '"data": null, '
      '"exception": {'
        '"exception": "MockExceptionWithArgs", '
        '"message": "test", '
        '"args": ["test"], '
        '"kwargs": {}, '
        '"module": "tests.rpc.test_formatters"'
       '}'
   '}')


def test_format_rpc_response_with_exception_with_getkwargs():
    exc = MockExceptionWithGetKwargs("test", "arg1", "test2")
    ret = format_rpc_response(None, exception=exc)
    assert ret == ('{'
      '"data": null, '
      '"exception": {'
        '"exception": "MockExceptionWithGetKwargs", '
        '"message": "test", '
        '"args": ["test", "arg1"], '
        '"kwargs": {"arg2": "test2"}, '
        '"module": "tests.rpc.test_formatters"'
       '}'
   '}')


def test_format_rpc_response_builting_exception():
    exc = ValueError("test")
    ret = format_rpc_response(None, exception=exc)
    assert ret == ('{'
       '"data": null, '
       '"exception": {'
       '"exception": "ValueError", '
       '"message": "test", '
       '"args": ["test"], '
       '"kwargs": {}, '
       '"module": null'
       '}'
    '}')


def test_parse_rpc_response():
    msg = ('{'
        '"data": {'
            '"response": "this is a response"'
        '}, '
        '"exception": null'
   '}')
    res = parse_rpc_response(msg)
    assert res == {"response": "this is a response"}


def test_parse_rpc_response_with_exception():
    msg = ('{'
      '"data": null, '
      '"exception": {'
        '"exception": "MockExceptionWithGetKwargs", '
        '"message": "test, test2", '
        '"args": ["test", "arg1"], '
        '"kwargs": {"arg2": "test2"}, '
        '"module": "tests.rpc.test_formatters"'
       '}'
   '}')
    res = parse_rpc_response(msg)
    assert type(res) == MockExceptionWithGetKwargs
