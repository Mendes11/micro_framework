import inspect
from concurrent.futures.process import _ExceptionWithTraceback
from importlib import import_module

from micro_framework.dependencies import Dependency


def get_class_dependencies(_class, config):
    dependencies = inspect.getmembers(
        _class, lambda x: isinstance(x, Dependency)
    )
    for name, dependency in dependencies:
        dependency.bind(config)
    return dict(dependencies)


class Worker:
    """
    Worker class that call the task.

    The task can either be a function path or a class. In case it is a class,
    it will consider the class instance a callable or it will call the
    'method_name' if provided.

    The Worker initiates the dependencies and translators.

    If the target is a class, it also search for class Dependencies to be
    initialized.

    """
    def __init__(self, target, dependencies, translators,
                 config, *fn_args, _meta=None, method_name=None, **fn_kwargs):
        self.target = target
        self.method_name = method_name
        self.config = config
        self.fn_args = fn_args
        self.fn_kwargs = fn_kwargs
        self._dependencies = dependencies or {}
        self._translators = translators or []
        self.result = None
        self.exception = None
        self._meta = _meta or {} # Content shared by extensions

    def get_callable(self):
        *module, callable = self.target.split('.')
        callable = getattr(import_module('.'.join(module)), callable)
        return callable

    def call_dependencies(self, dependencies, action, *args, **kwargs):
        ret = {}
        for name, dependency in dependencies.items():
            ret[name] = getattr(dependency, action)(*args, **kwargs)
        return ret

    def instanciate_class(self, _class, dependencies):
        instance = _class()
        for dependency_name, dependency in dependencies.items():
            setattr(instance, dependency_name, dependency)
        return instance

    def call_task(self, fn, *fn_args, **fn_kwargs):
        try:
            self.result = fn(*fn_args, **fn_kwargs)
        except Exception as exc:
            if self.config.get('WORKER_MODE', 'thread') == 'process':
                # Trick from concurrent.futures to keep the traceback in
                # process type worker.
                self.exception = _ExceptionWithTraceback(exc, exc.__traceback__)
            else:
                self.exception = exc

            self.call_dependencies(
                self._dependencies, 'after_call', self, self.result, exc
            )
            self.call_dependencies(
                self._class_dependencies, 'after_call', self, self.result, exc
            )

        self.call_dependencies(
            self._dependencies, 'after_call', self, self.result, None
        )
        self.call_dependencies(
            self._class_dependencies, 'after_call', self, self.result, None
        )
        return self.result

    def run(self):
        callable = self.target
        if isinstance(self.target, str):
            callable = self.get_callable()

        self._class_dependencies = {}
        if inspect.isclass(callable):
            self._class_dependencies = get_class_dependencies(
                callable, self.config
            )

        # Setup Dependency Providers
        self.call_dependencies(self._dependencies, 'setup')
        self.call_dependencies(self._class_dependencies, 'setup')

        # Obtaining Dependencies from Providers
        dependencies = self.call_dependencies(
            self._dependencies, 'get_dependency', self
        )
        class_dependencies = self.call_dependencies(
            self._class_dependencies, 'get_dependency', self
        )

        # Translating Messages if there is any translator
        self.translated_args = self.fn_args
        self.translated_kwargs = self.fn_kwargs
        for translator in self._translators:
            self.translated_args, self.translated_kwargs = translator.translate(
                *self.fn_args, **self.fn_kwargs
            )

        # Calling Before Call for the Dependency Providers
        self.call_dependencies(self._dependencies, 'before_call', self)
        self.call_dependencies(self._class_dependencies, 'before_call', self)

        function = callable
        if inspect.isclass(callable):
            instance = self.instanciate_class(callable, class_dependencies)

            function = instance
            if self.method_name:
                function = getattr(instance, self.method_name)

        fn_kwargs = {**self.translated_kwargs, **dependencies}
        self.call_task(function, *self.translated_args, **fn_kwargs)
        return self


class CallbackWorker(Worker):
    def __init__(self, callback_target, original_worker):
        self.callback_target = callback_target
        self.original_worker = original_worker
        args = (*original_worker.translated_args, original_worker.exception)
        kwargs = original_worker.translated_kwargs
        # We use the already translated content but the dependencies will
        # have to be re-created due to the pickling problem of Processing.
        super(CallbackWorker, self).__init__(
            callback_target, original_worker._dependencies, None,
            original_worker.config, *args, _meta=original_worker._meta,
            **kwargs)
