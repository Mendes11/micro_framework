import logging
from wsgiref.simple_server import make_server

from prometheus_client import make_wsgi_app, REGISTRY, Gauge, Counter, Summary, \
    Info
from prometheus_client.exposition import ThreadingWSGIServer, _SilentHandler

from micro_framework.extensions import Extension

logger = logging.getLogger(__name__)


class PrometheusMetricServer(Extension):
    singleton = True

    def __init__(self):
        self.started = False

    async def setup(self):
        self.config = self.runner.config['METRICS']
        self.enabled = self.runner.config['ENABLE_METRICS']
        self.started = False

    async def start(self):
        """
        Starts a WSGI server exactly like prometheus_client.start_http_server
        except that we use our runner to spawn the task instead of a Thread
        alone.
        """
        # TODO Change to uvicorn
        if self.enabled:
            app = make_wsgi_app(REGISTRY)
            self.httpd = make_server(
                self.config['HOST'],
                self.config['PORT'],
                app,
                ThreadingWSGIServer,
                handler_class=_SilentHandler
            )
            logger.info(
                "Prometheus Metrics Server Started\n\t-Listening at"
                f" {self.config['HOST']}:{self.config['PORT']}"
            )
            self.started = True
            self.task = self.runner.event_loop.run_in_executor(
                None, self.httpd.serve_forever
            )

    async def stop(self):
        if self.started:
            self.httpd.shutdown()


# Runner Metrics
tasks_number = Gauge(
    'tasks_number', 'Number of Tasks being run at the moment',
    ["context", 'route']
)

tasks_counter = Counter(
    'tasks_number', 'Total number of tasks',
    ["context", "route"]
)

tasks_failures_counter = Counter(
    'failed_tasks', 'Total number of tasks that have failed',
    ["context", "route"]
)

tasks_duration = Summary(
    'task_duration_seconds', 'Duration time of a task',
    ["context", "route"]
)

info = Info(
    'service', 'Information regarding the service', ["context"]
)

available_workers = Gauge(
    "available_workers", "Number of Workers available",
    ["context"]
)
