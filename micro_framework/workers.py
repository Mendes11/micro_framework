import asyncio
import inspect
import logging
from threading import Thread

from functools import partial

logger = logging.getLogger(__name__)


def _start_worker_event_loop(event_loop):
    """
    Starts an event-loop and set is as the event-loop for the current process.

    The event-loop would be already running inside a Thread, returned from
    the function.

    It should be called only for process workers that spawn a new process
    and therefore can't access the main event-loop
    """
    event_loop_thread = Thread(
        target=event_loop.run_forever, daemon=True
    )
    event_loop_thread.start()

    return event_loop, event_loop_thread


def executor_task(func, *fn_args, **fn_kwargs):
    """
    Function to execute a sync task using a spawner.

    :param func: Function to be called
    :param fn_args: Function args
    :param fn_kwargs: Function kwargs
    :return: Function Result
    :raises: Function Exceptions
    """
    # TODO This fn might be responsibility of target module
    event_loop_thread = None
    event_loop = asyncio.new_event_loop()

    try:
        # Run the function but starts a running event loop before,
        # in order to the Target methods that are asyncio.
        # TODO this is why this fn seems like it belongs to targets.py
        event_loop, event_loop_thread = _start_worker_event_loop(event_loop)
        asyncio.set_event_loop(event_loop)
        result = func(*fn_args, **fn_kwargs)
    finally:
        event_loop.close()

        if event_loop_thread:
            event_loop_thread.join()  # wait until completely closed.
    return result


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

    async def call_task(self, spawner, mounted_target, *fn_args, **fn_kwargs):
        try:
            is_coroutine = inspect.iscoroutinefunction(mounted_target.run)
            if spawner and is_coroutine:
                raise TypeError(
                    f"{mounted_target} is an Async Target but a {spawner} "
                    f"spawner was provided. Please either use an 'asyncio' "
                    f"worker_mode or transform the coroutine into a "
                    f"function/method"
                )
            elif spawner:  # Run it outside the event-loop
                event_loop = asyncio.get_event_loop()
                fn = partial(
                    executor_task, mounted_target.run, *fn_args, **fn_kwargs
                )

                self.result = await event_loop.run_in_executor(spawner, fn)

            else:  # When no spawner is given, we consider it an asyncio target.
                self.result = await mounted_target.run(
                    *fn_args, **fn_kwargs
                )

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
