import inspect
import time
import uuid

from micro_framework.extensions import Extension
from micro_framework.routes import Route
from timeit import default_timer as timer


class Entrypoint(Extension):
    def __init__(self, route: Route):
        self.route = route
        self.pending_entries = {}

    def setup(self):
        self.bind_to_routes()

    def bind_to_routes(self):
        for name, route in inspect.getmembers(
                self, lambda x: isinstance(x, Route)):
            route.bind_entrypoint(self)

    def new_entry(self, entry_id, *args, **kwargs):
        self.route.start_route(entry_id, *args, **kwargs)

    def on_success_route(self, worker):
        pass

    def on_failed_route(self, worker):
        pass


class TimeEntrypoint(Entrypoint):
    def __init__(self, interval, route: Route):
        self.interval = interval
        super(TimeEntrypoint, self).__init__(route)

    def start(self):
        self.runner.spawn_extension(self, self.run)

    def on_success_route(self, worker):
        print("Successfull worker")

    def on_failed_route(self, worker):
        print("Failed worker")

    def run(self):
        while True:
            t1 = timer()
            self.new_entry(uuid.uuid4())
            elapsed_time = timer() - t1
            sleep_time = self.interval - elapsed_time
            if sleep_time > 0:
                time.sleep(sleep_time)
