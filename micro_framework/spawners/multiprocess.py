import logging
import signal


from loky.process_executor import ProcessPoolExecutor, BrokenProcessPool

from micro_framework.spawners.base import Spawner
from micro_framework.exceptions import BrokenSpawner


logger = logging.getLogger(__name__)


def process_initializer():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


class ProcessPoolSpawner(Spawner, ProcessPoolExecutor):
    def __init__(self, config):
        timeout = config.get("PROCESS_WORKER_TIMEOUT")
        super(ProcessPoolSpawner, self).__init__(
            max_workers=config.get("MAX_WORKERS"),
            timeout=timeout,
            initializer=process_initializer
        )

    def submit(self, *args, **kwargs):
        try:
            return super(ProcessPoolSpawner, self).submit(*args, **kwargs)
        except BrokenProcessPool as exc:
            raise BrokenSpawner from exc

    def shutdown(self, **kwargs) -> None:
        logger.info(
            "Process Worker shutdown initiated, wait until all pending tasks "
            "are consumed."
        )
        super(ProcessPoolSpawner, self).shutdown(**kwargs)
