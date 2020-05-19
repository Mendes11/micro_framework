# from kombu import Exchange, Connection
# from kombu.pools import producers
#
# from micro_framework.extensions import Dependency
#
#
# def dispatch(amqp_uri, service_name, event, payload, transport_options):
#     exchange = Exchange(service_name, type='direct')
#     connection = Connection(amqp_uri, transport_options=transport_options)
#     with producers[connection].acquire(block=True) as producer:
#         producer.publish(payload, exchange=exchange, routing_key=event)
#
#
# class Producer(Dependency):
#     def __init__(self, confirm_publish=True):
#         self.confirm_publish = confirm_publish
#
#     @property
#     def exchange(self):
#         return Exchange(self.runner.config['SERVICE_NAME'], type='direct')
#
#     @property
#     def amqp_uri(self):
#         return self.runner.config['AMQP_URI']
#
#     @property
#     def transport_options(self):
#         return {
#             'confirm_publish': self.confirm_publish
#         }
#
#     @property
#     def connection(self):
#         return Connection(self.amqp_uri,
#                           transport_options=self.transport_options)
#
#     def get_dependency(self):
#         def publish(payload):
#             with producers[self.connection].acquire(block=True) as producer:
#                 return producer.publish(payload, exchange=self.exchange)
#         return publish
