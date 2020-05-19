from concurrent.futures import ProcessPoolExecutor

from micro_framework.spawners.base import Spawner


class ProcessSpawner(Spawner):
    def __init__(self, n_workers=None):
        self.n_workers = n_workers
        self.pool = ProcessPoolExecutor(max_workers=n_workers)
