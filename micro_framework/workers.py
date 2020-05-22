from importlib import import_module


class Worker:
    def __init__(self, entry_id, function_path, dependencies, translators,
                 *fn_args, **fn_kwargs):
        self.entry_id = entry_id
        self.function_path = function_path
        self.fn_args = fn_args
        self.fn_kwargs = fn_kwargs
        self.dependencies = dependencies or {}
        self.translators = translators or []
        self.result = None
        self.exception = None

    def get_callable(self):
        *module, function = self.function_path.split('.')
        function = getattr(import_module('.'.join(module)), function)
        return function

    def call_dependencies(self, action, *args, **kwargs):
        ret = {}
        for name, dependency in self.dependencies.items():
            ret[name] = getattr(dependency, action)(*args, **kwargs)
        return ret

    def run(self):
        self.function = self.get_callable()
        self.call_dependencies('setup')
        self.injected_dependencies = self.call_dependencies(
            'get_dependency', self
        )
        result = None
        for translator in self.translators:
            self.args, self.kwargs = translator.translate(
                *self.fn_args, **self.fn_kwargs
            )
        try:
            # Updating kwargs
            fn_kwargs = {**self.fn_kwargs, **self.injected_dependencies}
            self.result = self.function(*self.fn_args, **fn_kwargs)
        except Exception as exc:
            self.exception = exc
            self.call_dependencies('after_call', result, exc, self)
            del self.dependencies
            raise
        self.call_dependencies('after_call', result, None, self)
        del self.dependencies
        return self
