import time

from micro_framework.extensions import Extension
from micro_framework.routes import Route
from timeit import default_timer as timer


class Entrypoint(Extension):
    def __init__(self, success_route: Route, failure_route: Route = None):
        self.success_route = success_route
        self.failure_route = failure_route


class TimeEntrypoint(Entrypoint):
    def __init__(
            self, interval, success_route: Route, failure_route: Route = None
    ):
        self.interval = interval
        super(TimeEntrypoint, self).__init__(success_route, failure_route)

    def start(self):
        self.runner.spawn_extension(self, self.run)

    def run(self):
        while True:
            t1 = timer()
            self.success_route.start_route(None, None)
            elapsed_time = timer() - t1
            sleep_time = self.interval - elapsed_time
            if sleep_time > 0:
                time.sleep(sleep_time)
