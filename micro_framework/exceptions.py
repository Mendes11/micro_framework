class FrameworkException(Exception):
    pass


class PoolStopped(Exception):
    pass

class ExtensionIsStopped(FrameworkException):
    pass


class RPCTargetDoesNotExist(FrameworkException):
    pass


class RPCException(FrameworkException):
    def __init__(self, exception_dict):
        self.exc_type = exception_dict['exception']
        self.value = exception_dict['message']
        if self.value:
            message = f"{self.exc_type} {self.value}"
        else:
            message = self.exc_type
        super(RPCException, self).__init__(message)


class MaxConnectionsReached(FrameworkException):
    pass


class RPCAsyncResponse(FrameworkException):
    """
    Exception used to differentiate when a RPC call should return or not

    We raise an exception instead of returning None because None could be the
    desired response.
    """
