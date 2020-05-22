from concurrent.futures.process import _ExceptionWithTraceback
from importlib import import_module


class Worker:
    def __init__(self, function_path, dependencies, translators,
                 config, *fn_args, **fn_kwargs):
        self.function_path = function_path
        self.config = config
        self.fn_args = fn_args
        self.fn_kwargs = fn_kwargs
        self._dependencies = dependencies or {}
        self._translators = translators or []
        self.result = None
        self.exception = None

    def get_callable(self):
        *module, function = self.function_path.split('.')
        function = getattr(import_module('.'.join(module)), function)
        return function

    def call_dependencies(self, action, *args, **kwargs):
        ret = {}
        for name, dependency in self._dependencies.items():
            ret[name] = getattr(dependency, action)(*args, **kwargs)
        return ret

    def run(self):
        self.function = self.get_callable()
        self.call_dependencies('setup')
        dependencies = self.call_dependencies(
            'get_dependency', self
        )
        self.translated_args = self.fn_args
        self.translated_kwargs = self.fn_kwargs
        for translator in self._translators:
            self.translated_args, self.translated_kwargs = translator.translate(
                *self.fn_args, **self.fn_kwargs
            )
        self.call_dependencies('before_call', self)
        try:
            # Updating kwargs
            fn_kwargs = {**self.translated_kwargs, **dependencies}
            self.result = self.function(*self.translated_args, **fn_kwargs)
        except Exception as exc:
            if self.config.get('WORKER_MODE', 'thread') == 'process':
                # Trick from concurrent.futures to keep the traceback in
                # process type worker.
                self.exception = _ExceptionWithTraceback(exc, exc.__traceback__)
            else:
                self.exception = exc
            self.call_dependencies('after_call', self, self.result, exc)
        self.call_dependencies('after_call', self, self.result, None)
        return self