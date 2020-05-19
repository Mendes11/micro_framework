from logging import getLogger

logger = getLogger(__name__)


class Spawner:
    def spawn(self, target_fn, callback, *fn_args, **fn_kwargs):
        fn_args = fn_args or tuple()
        fn_kwargs = fn_kwargs or dict()
        logger.debug(f"Spawning {target_fn.__name__}")
        future = self.pool.submit(target_fn, *fn_args, **fn_kwargs)
        logger.debug(f"Target {target_fn.__name__} spawned")
        if callback:
            future.add_done_callback(callback)
        return future

    def stop(self, wait=True):
        self.pool.shutdown(wait=wait)

    def kill(self):
        self.pool.shutdown(wait=False)
