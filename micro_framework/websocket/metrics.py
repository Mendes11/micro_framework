from prometheus_client import Gauge

active_connections = Gauge(
    'ws_active_connections', 'Number of Active Connections'
)

