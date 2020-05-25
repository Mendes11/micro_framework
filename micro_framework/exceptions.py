class FrameworkException(Exception):
    pass


class PoolStopped(Exception):
    pass

class ExtensionIsStopped(FrameworkException):
    pass


class BackOffLimitReached(FrameworkException):
    pass


class BackOffIntervalNotReached(FrameworkException):
    pass

