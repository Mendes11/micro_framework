from concurrent.futures import ThreadPoolExecutor

from micro_framework.spawners.base import Spawner


class ThreadSpawner(Spawner):
    def __init__(self, n_workers=None):
        self.n_workers = n_workers
        self.pool = ThreadPoolExecutor(max_workers=n_workers)
