import logging
from logging import getLogger

from micro_framework.amqp.routes import EventRoute
from micro_framework.runner import Runner

config = {
    'AMQP_URI': 'amqp://localhost:5672',
    'MAX_WORKERS': 3,
    'SERVICE_NAME': 'my_service',
    'WORKER_MODE': 'thread'
}

logger = getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(message)s')

routes = [
    EventRoute('my_exchange', 'my_routing_key', 'tasks.test'),
    # EventRoute('my_exchange', 'my_routing_key', 'tasks.test2'),
]

runner = Runner(routes, config)
runner.start()
