import logging
import signal


from loky.process_executor import ProcessPoolExecutor

from micro_framework.spawners.base import Spawner


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

    def shutdown(self, **kwargs) -> None:
        logger.info(
            "Process Worker shutdown initiated, wait until all pending tasks "
            "are consumed."
        )
        super(ProcessPoolSpawner, self).shutdown(**kwargs)
