from micro_framework.extensions import Extension


class Dependency(Extension):
    """
    Kind of extension that injects a dependency into the function.
    """

    def bind(self, config):
        self.config = config

    def before_call(self, worker):
        """
        Treatment before the actual function call.
        """
        pass

    def get_dependency(self, worker):
        """
        Injects the dependency.
        """
        pass

    def after_call(self, worker, result, exc):
        """
        Cleanup after the function has finished or raised.
        """
        pass


class Config(Dependency):
    def get_dependency(self, worker):
        return self.config
