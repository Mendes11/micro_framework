import logging
import multiprocessing
import signal
import threading
from concurrent.futures import _base, ProcessPoolExecutor
from concurrent.futures.process import _ExceptionWithTraceback
from multiprocessing.context import Process
from multiprocessing.queues import Queue
from queue import Empty

from micro_framework.exceptions import PoolStopped
from micro_framework.spawners.base import Spawner, Task, CallItem


logger = logging.getLogger(__name__)

def process_initializer():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def process_worker(call_queue, result_queue):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    while True:
        call_item = call_queue.get()
        if call_item is None:
            # Shutdown signal
            return
        function = call_item.target
        fn_args = call_item.target_args or tuple()
        fn_kwargs = call_item.target_kwargs or {}
        result = exc = None
        try:
            result = function(*fn_args, **fn_kwargs)
        except Exception as _exc:
            exc = _ExceptionWithTraceback(_exc, _exc.__traceback__)
        result_queue.put((call_item.task_id, result, exc))


class ProcessSpawner(Spawner, _base.Executor):
    def __init__(self, max_workers=3):
        self.max_workers = max_workers
        self._pool = None
        self.ctx = multiprocessing.get_context()
        self.write_queue = Queue(ctx=self.ctx)
        self.read_queue = Queue(ctx=self.ctx)
        self.call_queue = Queue(ctx=self.ctx)
        self.shutdown_pool = False
        self.shutdown_signal = False
        self.shutdown_lock = threading.Lock()
        self.initiate_pool()
        self.initiate_queue_task()
        self.total_tasks = 0
        self._tasks = {}

    def initiate_queue_task(self):
        self._queue_handler_task = threading.Thread(
            target=self._handle_queues)
        self._queue_handler_task.daemon = True
        self._queue_handler_task.start()

    def initiate_pool(self):
        self._pool = [
            Process(
                target=process_worker,
                args=(self.call_queue, self.read_queue)
            )
            for _ in range(self.max_workers)
        ]
        for process in self._pool:
            process.daemon = True
            process.start()

    def create_task(self, target, *target_args, **target_kwargs):
        task = Task(self.total_tasks + 1, target, *target_args, **target_kwargs)
        return task

    def submit(self, target_fn, *args, **kwargs):
        with self.shutdown_lock:
            if self.shutdown_signal:
                raise PoolStopped("This pool is being shutdown.")
        task = self.create_task(target_fn, *args, **kwargs)
        self._tasks[task.task_id] = task
        self.write_queue.put(task.task_id)
        self.read_queue.put(None)  # Wake up queue handler
        self.total_tasks += 1
        return task.future

    def shutdown(self, wait=True):
        with self.shutdown_lock:
            self.shutdown_signal = True
        if wait:
            logger.info("Gracefully shutting down workers...")
            while not self.call_queue.empty():
                try:
                    self.call_queue.get(timeout=1)
                except Empty:
                    continue
            logger.debug("Call Queue Emptied.\nSending signal to stop process")
            for process in self._pool:
                self.call_queue.put(None)
            logger.debug("Signal sent. Joining Processes to shutdown.")
            for process in self._pool:
                process.join()  # Wait process to finish
                process.close()
        else:
            logger.info("Terminating workers.")
            for process in self._pool:
                process.terminate()
        logger.debug("Sending signal to stop queue_handler_task")
        # This will trigger a sleeping _queue_handler_task
        with self.shutdown_lock:
            self.shutdown_pool = True
        self.read_queue.put(None)
        self._queue_handler_task.join()
        logger.debug("Pool Stopped.")

    def _send_task_to_process(self):
        while True:
            try:
                task_id = self.write_queue.get(block=False)
            except Empty:
                return
            self.call_queue.put(
                CallItem.from_task(self._tasks[task_id])
            )

    def _read_task_result(self):
        result = self.read_queue.get(block=True)
        if result is None:
            return False
        task_id, result, exc = result
        future = self._tasks.pop(task_id).future
        if not exc:
            future.set_result(result)
        else:
            future.set_exception(exc)
        return True

    def _handle_queues(self):
        while True:
            self._send_task_to_process()
            result = self._read_task_result()
            with self.shutdown_lock:
                if self.shutdown_pool and not result:
                    self.read_queue.close()
                    self.write_queue.close()
                    self.call_queue.close()
                    break


class ProcessPoolSpawner(Spawner, ProcessPoolExecutor):
    def __init__(self, max_workers=None, mp_context=None, initializer=None,
                 initargs=()):
        if initializer is None:
            initializer = process_initializer
        super(ProcessPoolSpawner, self).__init__(max_workers, mp_context,
                                                 initializer, initargs)

    def shutdown(self, wait: bool = ...) -> None:
        logger.info("Greedy Worker shutdown initiated, wait until all pending "
                    "tasks are consumed.")
        super(ProcessPoolSpawner, self).shutdown(wait)
