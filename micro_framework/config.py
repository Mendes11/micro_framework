from collections import UserDict

DEFAULT_CONFIG = dict(
    MAX_WORKERS=3,
    WORKER_MODE='process',
    MAX_TASKS_PER_CHILD=None,
    AMPQ_URI='amqp://guest:guest@localhost:5672',
    METRICS={
        'HOST': '0.0.0.0',
        'PORT': 8000,
    },
    WEBSOCKET_IP='0.0.0.0',
    WEBSOCKET_PORT=8765,
)


class FrameworkConfig(UserDict):
    """
    Exposes the user configurations but also return the System Default
    configurations when a config is set by the user
    """

    def __init__(self, *args, default_config=None, **kwargs):
        if default_config is None:
            default_config = DEFAULT_CONFIG
        self.default_config = default_config
        super(FrameworkConfig, self).__init__(*args, **kwargs)

    def __getitem__(self, item):
        if item not in self.default_config:
            return super(FrameworkConfig, self).__getitem__(item)

        default_value = self.default_config.get(item)
        value = self.data.get(item, default_value)
        if isinstance(default_value, dict):
            return FrameworkConfig(value, default_config=default_value)
        return value
