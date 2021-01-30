import logging

from pathos.multiprocessing import ProcessPool

from micro_framework.spawners.base import Spawner


logger = logging.getLogger(__name__)


class ProcessPoolSpawner(Spawner, ProcessPool):
    def __init__(self, max_workers=None, mp_context=None, initargs=(),
                 **kwargs):
        super(ProcessPoolSpawner, self).__init__(max_workers)

    def shutdown(self, wait: bool = ...) -> None:
        logger.info("Greedy Worker shutdown initiated, wait until all pending "
                    "tasks are consumed.")
        super(ProcessPoolSpawner, self).shutdown(wait)

    def spawn(self, target_fn, *fn_args, callback=None, **fn_kwargs):
        future = self.apipe(lambda x: x * x, 5)
        return future