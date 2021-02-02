from micro_framework.exceptions import FrameworkException


class RouteNotFound(FrameworkException):
    pass


class DuplicatedListener(FrameworkException):
    pass

