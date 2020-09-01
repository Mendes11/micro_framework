from kombu import Exchange, Connection
from kombu.pools import producers

from micro_framework.dependencies import Dependency

default_transport_options = {
    'confirm_publish': True
}

def dispatch(amqp_uri, service_name, event, payload,
             transport_options=None, headers=None, exchange_type='direct',
             exchange=None):
    if transport_options is None:
        transport_options = default_transport_options
    if not exchange:
        exchange = Exchange(service_name, type=exchange_type)
    connection = Connection(amqp_uri, transport_options=transport_options)
    with producers[connection].acquire(block=True) as producer:
        producer.publish(payload, exchange=exchange, routing_key=event,
                         headers=headers)


class Producer(Dependency):
    def __init__(self, confirm_publish=True, service_name=None):
        self.service_name = service_name or self.config['SERVICE_NAME']
        self.confirm_publish = confirm_publish

    @property
    def exchange(self):
        exchange_name = f"{self.service_name}.events" # Nameko Compatible
        return Exchange(exchange_name, type='topic', auto_delete=True)

    @property
    def amqp_uri(self):
        return self.config['AMQP_URI']

    @property
    def transport_options(self):
        return {
            'confirm_publish': self.confirm_publish
        }

    @property
    def connection(self):
        return Connection(self.amqp_uri,
                          transport_options=self.transport_options)

    def get_dependency(self, worker):
        exchange = self.exchange
        def publish(event_name, payload):
            with producers[self.connection].acquire(block=True) as producer:
                producer.maybe_declare(exchange)
                return producer.publish(payload, exchange=exchange,
                                        routing_key=event_name)
        return publish
