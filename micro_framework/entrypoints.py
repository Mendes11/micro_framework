import inspect
import logging
import time
import uuid
from datetime import datetime
from timeit import default_timer as timer

from memory_profiler import profile

from micro_framework.exceptions import PoolStopped
from micro_framework.extensions import Extension
from micro_framework.routes import Route

logger = logging.getLogger(__name__)


class Entrypoint(Extension):
    def __init__(self, route: Route):
        self.route = route

    def setup(self):
        self.bind_to_routes()

    def bind_to_routes(self):
        for name, route in inspect.getmembers(
                self, lambda x: isinstance(x, Route)):
            route.bind_entrypoint(self)

    def new_entry(self, entry_id, *args, **kwargs):
        try:
            self.route.start_route(entry_id, *args, **kwargs)
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
    def __init__(self, interval, route: Route):
        self.interval = interval
        self.running = False
        super(TimeEntrypoint, self).__init__(route)

    def start(self):
        self.running = True
        self.runner.spawn_extension(self, self.run)

    def on_finished_route(self, entry_id, worker):
        if worker.exception:
            raise worker.exception

    def run(self):
        while self.running:
            t1 = timer()
            entry_id = uuid.uuid4()
            now = datetime.utcnow()
            self.new_entry(entry_id, now.isoformat())
            elapsed_time = timer() - t1
            sleep_time = self.interval - elapsed_time
            if sleep_time > 0:
                time.sleep(sleep_time)

    def stop(self):
        self.running = False