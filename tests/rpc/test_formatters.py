from micro_framework.rpc.formatters import import_exception, \
    format_rpc_response, parse_rpc_response
from micro_framework.exceptions import MaxConnectionsReached, RPCException

class MockException(Exception):
    pass

class MockExceptionWithArgs(Exception):
    def __init__(self, arg1=None, arg2=None):
        self.arg1 = arg1
        self.arg2 = arg2
        super(MockExceptionWithArgs, self).__init__(arg1)

class MockExceptionWithState(MockExceptionWithArgs):
    def __getstate__(self):
        return {"arg1": self.arg1, "arg2": self.arg2}

    def __setstate__(self, state):
        self.arg1 = state.get("arg1")
        self.arg2 = state.get("arg2")

    def __str__(self):
        return f"{self.arg1}, {self.arg2}"


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


def test_import_exception_custom_no_state():
    exc = import_exception({
        "exception": "MockExceptionWithArgs",
        "message": "Some Message",
        "module": MockException.__module__
    })
    assert type(exc) == MockExceptionWithArgs
    assert exc.arg1 == "Some Message"
    assert exc.arg2 is None
    assert str(exc) == "Some Message"

def test_import_exception_with_setstate():
    exc = import_exception({
        "exception": "MockExceptionWithState",
        "message": "1",
        "module": MockException.__module__,
        "state": {"arg1": 1, "arg2": 2}
    })
    assert type(exc) == MockExceptionWithState
    assert exc.arg1 == 1
    assert exc.arg2 == 2
    assert str(exc) == "1, 2"

def test_import_exception_no_state_given():
    exc = import_exception({
        "exception": "MockExceptionWithState",
        "message": "Test",
        "module": MockException.__module__,
    })
    assert type(exc) == MockExceptionWithState
    assert exc.arg1 == "Test"
    assert exc.arg2 is None
    assert str(exc) == "Test, None"

def test_import_exception_no_module_found():
    exc = import_exception({
        "exception": "ServerSpecificException",
        "message": "Some Message",
        "module": "inexistent.module",
        "state": {"arg1": "Not used"}
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


def test_format_rpc_response_with_exception_no_getstate():
    exc = MockExceptionWithArgs("test", "test2")
    ret = format_rpc_response(None, exception=exc)
    assert ret == ('{'
      '"data": null, '
      '"exception": {'
        '"exception": "MockExceptionWithArgs", '
        '"message": "test", "state": null, '
        '"module": "tests.rpc.test_formatters"'
       '}'
   '}')


def test_format_rpc_response_with_exception_with_getstate():
    exc = MockExceptionWithState("test", "test2")
    ret = format_rpc_response(None, exception=exc)
    assert ret == ('{'
      '"data": null, '
      '"exception": {'
        '"exception": "MockExceptionWithState", '
        '"message": "test, test2", '
        '"state": {"arg1": "test", "arg2": "test2"}, '
        '"module": "tests.rpc.test_formatters"'
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
        '"exception": "MockExceptionWithState", '
        '"message": "test, test2", '
        '"state": {"arg1": "test", "arg2": "test2"}, '
        '"module": "tests.rpc.test_formatters"'
       '}'
   '}')
    res = parse_rpc_response(msg)
    assert type(res) == MockExceptionWithState
