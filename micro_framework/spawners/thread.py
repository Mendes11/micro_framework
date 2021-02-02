import logging
import multiprocessing
import threading
from concurrent.futures import _base, ThreadPoolExecutor
from multiprocessing.queues import Queue
from queue import Empty

from micro_framework.exceptions import PoolStopped
from micro_framework.spawners.base import Spawner, Task

logger = logging.getLogger(__name__)


def thread_worker(executor, write_queue, stop_event: threading.Event):
    while not stop_event.isSet():
        task_id = write_queue.get()
        with executor.shutdown_lock:
            if executor.shutdown_signal or task_id is None:
                # Shutdown signal
                return
        task = executor._tasks.pop(task_id, None)  # Remove to avoid Mem. inflating.
        if task is not None:
            task.run()


class ThreadSpawner(Spawner, _base.Executor):
    def __init__(self, config):
        self.max_workers = config.get("MAX_WORKERS")
        self._pool = []
        self._stop_map = {}
        self.ctx = multiprocessing.get_context()
        self.write_queue = Queue(ctx=self.ctx)
        self.shutdown_signal = False
        self.shutdown_lock = threading.Lock()
        self.initiate_pool()
        self.total_tasks = 0
        self._tasks = {}

    def _check_pool(self):
        """
        Verify the threads in the pool to spawn a new thread in case the
        count is different than the max_workers
        :return:
        """
        with self.shutdown_lock:
            if self.shutdown_signal:
                return
        logger.debug(f"Checking for dead threads in the pool. Currently "
                    f"{self._pool} threads in the pool.")

        for thread in self._pool:
            if not thread.is_alive():
                logger.debug(
                    f"Thread {thread} - PID: {thread.ident} is dead. Spawning "
                    f"a new thread to the pool."
                )
                self.stop_thread(thread)
                self.start_new_thread()

    def start_new_thread(self):
        evt = threading.Event()
        t = threading.Thread(
            target=thread_worker,
            args=(self, self.write_queue, evt)
        )
        t.daemon = True
        self._stop_map[t] = evt
        self._pool.append(t)
        t.start()
        return t

    def stop_thread(self, thread):
        self._pool.remove(thread)
        self._stop_map[thread].set()

    def initiate_pool(self):
        for _ in range(self.max_workers):
            self.start_new_thread()

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
        self.total_tasks += 1
        return task.future

    def shutdown(self, wait=True):
        with self.shutdown_lock:
            self.shutdown_signal = True
        # Threads don't have a terminate so in any case we would need to
        # gracefully stop it... The difference is that since it is a daemon,
        # we just skip joining the thread and leave to the parent process to
        # stop.
        logger.debug(f"Clearing Thread write queue "
                     f"({self.write_queue.qsize()})")
        while not self.write_queue.empty():  # Emptying the queue
            try:
                self.write_queue.get(timeout=1)
            except Empty:
                continue
        logger.debug("Sending stop signal to Threads")
        for thread in self._pool:
            self.write_queue.put(None)  # Signalling workers to stop
        if wait:
            logger.info("Gracefully shutting down thread workers...")
            for thread in self._pool:
                thread.join()  # Wait process to finish current task
        logger.debug("Thread Pool Finished")


class ThreadPoolSpawner(Spawner, ThreadPoolExecutor):
    def __init__(self, config):
        super(ThreadPoolSpawner, self).__init__(config.get("MAX_WORKERS"))

    def shutdown(self, **kwargs) -> None:
        logger.info("Greedy Worker shutdown initiated, wait until all pending "
                    "tasks are consumed.")
        super(ThreadPoolSpawner, self).shutdown(**kwargs)
