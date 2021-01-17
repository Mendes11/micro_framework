from prometheus_client import Gauge


unacked_rpc_messages = Gauge(
    'amqp_unacked_rpc_messages', 'Number of Pending RPC Messages',
    ["context", "route"]
)


unacked_event_messages = Gauge(
    'amqp_unacked_event_messages', 'Number of Pending Event Messages',
    ["context", "route"]
)
