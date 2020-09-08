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
        message = f"{self.exc_type} {self.value}"
        super(RPCException, self).__init__(message)