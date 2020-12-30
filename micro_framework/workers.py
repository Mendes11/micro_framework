import asyncio
import inspect
import logging
from concurrent.futures.process import _ExceptionWithTraceback
from functools import partial
from importlib import import_module

from micro_framework.dependencies import Dependency


logger = logging.getLogger(__name__)


async def get_class_dependencies(_class, config):
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
        self.finished = False
        self._meta = _meta or {} # Content shared by extensions

    async def get_callable(self):
        *module, callable = self.target.split('.')
        callable = getattr(import_module('.'.join(module)), callable)
        return callable

    async def call_dependencies(self, dependencies, action, *args, **kwargs):
        futures = []
        event_loop = asyncio.get_event_loop()
        names = []
        for name, dependency in dependencies.items():
            method = getattr(dependency, action)
            fn = partial(method, *args, **kwargs)
            futures.append(event_loop.run_in_executor(None, fn))
            names.append(name)
        results = await asyncio.gather(*futures)
        ret = {name: result for name, result in zip(names, results)}
        return ret

    async def instanciate_class(self, _class, dependencies):
        instance = _class()
        for dependency_name, dependency in dependencies.items():
            setattr(instance, dependency_name, dependency)
        return instance

    async def call_task(self, executor, fn, *fn_args, **fn_kwargs):
        try:
            if inspect.iscoroutinefunction(fn):
                self.result = await fn(*fn_args, **fn_kwargs)
            else:
                func = partial(fn, *fn_args, **fn_kwargs)
                event_loop = asyncio.get_event_loop()
                self.result = await event_loop.run_in_executor(executor, func)
        except Exception as exc:
            self.exception = exc
            logger.exception("")

        return self.result

    async def run(self, executor):
        callable = self.target
        if isinstance(self.target, str):
            callable = await self.get_callable()

        self._class_dependencies = {}
        if inspect.isclass(callable):
            self._class_dependencies = await get_class_dependencies(
                callable, self.config
            )

        # TODO Get dependencies from the function typing instead of the Route.
        # Then we will call the bind from here, not in the Route.

        # Setup Dependency Providers
        await self.call_dependencies(
            self._dependencies, 'setup_dependency', self
        )
        await self.call_dependencies(
            self._class_dependencies, 'setup_dependency', self
        )

        # Obtaining Dependencies from Providers
        dependencies = await self.call_dependencies(
            self._dependencies, 'get_dependency', self
        )
        class_dependencies = await self.call_dependencies(
            self._class_dependencies, 'get_dependency', self
        )

        # Translating Messages if there is any translator
        self.translated_args = self.fn_args
        self.translated_kwargs = self.fn_kwargs
        for translator in self._translators:
            self.translated_args, self.translated_kwargs = await translator.translate(
                *self.fn_args, **self.fn_kwargs
            )

        # Calling Before Call for the Dependency Providers
        await self.call_dependencies(self._dependencies, 'before_call', self)
        await self.call_dependencies(
            self._class_dependencies, 'before_call', self
        )

        function = callable
        if inspect.isclass(callable):
            instance = await self.instanciate_class(
                callable, class_dependencies
            )

            function = instance
            if self.method_name:
                function = getattr(instance, self.method_name)

        fn_kwargs = {**self.translated_kwargs, **dependencies}
        await self.call_task(
            executor, function, *self.translated_args, **fn_kwargs
        )
        await self.call_dependencies(
            self._dependencies, 'after_call', self, self.result, self.exception
        )
        await self.call_dependencies(
            self._class_dependencies, 'after_call', self, self.result,
            self.exception
        )
        self.finished = True
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
