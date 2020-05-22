class FrameworkException(Exception):
    pass


class PoolStopped(Exception):
    pass

class ExtensionIsStopped(FrameworkException):
    pass
