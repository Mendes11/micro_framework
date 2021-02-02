import signal
from concurrent.futures import _base
from logging import getLogger

from loky.backend import get_context

logger = getLogger(__name__)


class Spawner:
    """
    Mixin to Executors
    """

    def spawn(self, target_fn, *fn_args, callback=None, **fn_kwargs):
        fn_args = fn_args or tuple()
        fn_kwargs = fn_kwargs or dict()
        logger.debug(f"Spawning {target_fn.__name__}")
        future = self.submit(target_fn, *fn_args, **fn_kwargs)
        logger.debug(f"Target {target_fn.__name__} spawned")
        if callback:
            future.add_done_callback(callback)
        return future

    def stop(self, wait=True):
        self.shutdown(wait=wait)

    def kill(self):
        self.shutdown(wait=False)


def worker_init():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


class CallItem:
    def __init__(self, task_id, target, *target_args, **target_kwargs):
        self.task_id = task_id
        self.target = target
        self.target_args = target_args
        self.target_kwargs = target_kwargs

    @classmethod
    def from_task(cls, task):
        return cls(task.task_id, task.target, *task.target_args,
                   **task.target_kwargs)


class Task:
    def __init__(self, task_id, target, *target_args, **target_kwargs):
        self.task_id = task_id
        self.target = target
        self.target_args = target_args
        self.target_kwargs = target_kwargs
        self.future = _base.Future()

    def run(self):
        # Called by Thread Pool
        try:
            result = self.target(*self.target_args, **self.target_kwargs)
            self.future.set_result(result)
        except Exception as _exc:
            self.future.set_exception(_exc)


ctx = get_context()