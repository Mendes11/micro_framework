import multiprocessing
import threading
from concurrent.futures import _base
from multiprocessing.queues import Queue

from micro_framework.spawners.base import Spawner, Task


def thread_worker(executor, write_queue):
    while True:
        task_id = write_queue.get()
        with executor.shutdown_lock:
            if executor.shutdown_signal:
                # Shutdown signal
                return
        task = executor._tasks[task_id]
        task.run()


class ThreadSpawner(Spawner, _base.Executor):
    def __init__(self, max_workers=3):
        self.max_workers = max_workers
        self._pool = None
        self.ctx = multiprocessing.get_context()
        self.write_queue = Queue(ctx=self.ctx)
        self.shutdown_signal = False
        self.shutdown_lock = threading.Lock()
        self.initiate_pool()
        self.total_tasks = 0
        self._tasks = {}

    def initiate_pool(self):
        self._pool = [
            threading.Thread(
                target=thread_worker,
                args=(self, self.write_queue)
            )
            for _ in range(self.max_workers)
        ]
        for thread in self._pool:
            thread.daemon = True
            thread.start()

    def create_task(self, target, *target_args, **target_kwargs):
        task = Task(self.total_tasks + 1, target, *target_args, **target_kwargs)
        return task

    def submit(self, target_fn, *args, **kwargs):
        with self.shutdown_lock:
            if self.shutdown_signal:
                raise Exception("This pool is being shutdown.")
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
        while not self.write_queue.empty():  # Emptying the queue
            self.write_queue.get()
        for thread in self._pool:
            self.write_queue.put(None)  # Signalling workers to stop
        if wait:
            print("Gracefully shutting down workers...")
            for thread in self._pool:
                thread.join()  # Wait process to finish current task
        print("Finished shutting down")
