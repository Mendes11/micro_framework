import asyncio
import inspect
import logging

from functools import partial

logger = logging.getLogger(__name__)


class Worker:
    """
    Worker class that calls the target using an executor.

    The Task is a TargetFunction instance or a TargetClass instance.

    The worker is responsible to call the Target methods:
        * on_worker_setup
        * mount_target
        * on_worker_finished
    """

    def __init__(
            self, target, translators, config, *fn_args, _meta=None,
            **fn_kwargs
    ):
        self.target = target
        self.config = config
        self.fn_args = fn_args
        self.fn_kwargs = fn_kwargs
        self._translators = translators or []
        self.result = None
        self.exception = None
        self.finished = False
        self._meta = _meta or {}  # Content shared by extensions

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

    async def run(self, runner):
        self.runner = runner
        await self.target.on_worker_setup(self)

        # Translating Messages if there is any translator
        self.translated_args = self.fn_args
        self.translated_kwargs = self.fn_kwargs
        for translator in self._translators:
            self.translated_args, self.translated_kwargs = await translator.translate(
                *self.fn_args, **self.fn_kwargs
            )

        mounted_target = await self.target.mount_target(self)
        await self.call_task(
            runner.spawner, mounted_target, *self.translated_args,
            **self.translated_kwargs
        )
        self.finished = True
        await self.target.on_worker_finished(self)
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
            callback_target, None,
            original_worker.config, *args, _meta=original_worker._meta,
            **kwargs
        )
