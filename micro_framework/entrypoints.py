import logging
import time
import uuid
from datetime import datetime
from timeit import default_timer as timer

from micro_framework.exceptions import PoolStopped, ExtensionIsStopped
from micro_framework.extensions import Extension

logger = logging.getLogger(__name__)


class Entrypoint(Extension):
    route = None

    def bind_to_route(self, route):
        self.route = route

    def call_route(self, entry_id, *args, _meta=None, **kwargs):
        """
        Call the binded Route to start a new worker with the received content
        from the entrypoint.

        It returns the Instantiated Worker.

        :param entry_id:
        :param args:
        :param _meta:
        :param kwargs:
        :return:
        """
        try:
            return self.route.start_route(
                entry_id, *args, _meta=_meta, **kwargs
            )
        except PoolStopped:  # Unexpected error and we want to
            logger.info("New entry called when pool was already "
                        "signalled to stop.")
            self.on_failure(entry_id)
        except Exception:
            logger.exception("Failure when trying to start route for "
                             f"entrypoint: {self}")

    def on_finished_route(self, entry_id, worker):
        pass

    def on_failure(self, entry_id):
        pass


class TimeEntrypoint(Entrypoint):
    def __init__(self, interval):
        self.interval = interval
        self.running = False

    def start(self):
        self.running = True
        self.runner.spawn_extension(self, self.run)

    def run(self):
        last_time = timer()
        while self.running:
            elapsed_time = timer() - last_time
            sleep_time = self.interval - elapsed_time
            if sleep_time > 0:
                time.sleep(sleep_time)
            last_time = timer()
            entry_id = uuid.uuid4()
            now = datetime.utcnow()
            try:
                self.call_route(entry_id, now.isoformat())
            except (ExtensionIsStopped, PoolStopped):
                pass

    def stop(self):
        self.running = False
