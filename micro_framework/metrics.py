import logging
from wsgiref.simple_server import make_server

from prometheus_client import make_wsgi_app, REGISTRY, Gauge, Counter, Summary, \
    Info
from prometheus_client.exposition import ThreadingWSGIServer, _SilentHandler

from micro_framework.extensions import Extension

logger = logging.getLogger(__name__)


class PrometheusMetricServer(Extension):
    def setup(self):
        self.config = self.runner.config['METRICS']
        self.enabled = self.runner.config['ENABLE_METRICS']

    def start(self):
        """
        Starts a WSGI server exactly like prometheus_client.start_http_server
        except that we use our runner to spawn the task instead of a Thread
        alone.
        """
        if self.enabled:
            app = make_wsgi_app(REGISTRY)
            httpd = make_server(
                self.config['HOST'],
                self.config['PORT'],
                app,
                ThreadingWSGIServer,
                handler_class=_SilentHandler
            )
            self.runner.spawn_extension(self, httpd.serve_forever)
            logger.info(
                "Prometheus Metrics Server Started\n\t-Listening at"
                f" {self.config['HOST']}:{self.config['PORT']}"
            )


# Runner Metrics

tasks_number = Gauge(
    'tasks_number', 'Number of Tasks being run at the moment',
)

tasks_counter = Counter(
    'tasks_number', 'Total number of tasks'
)

tasks_failures_counter = Counter(
    'failed_tasks', 'Total number of tasks that have failed',
)

tasks_duration = Summary('task_duration_seconds', 'Duration time of a task')

info = Info('service', 'Information regarding the service')
