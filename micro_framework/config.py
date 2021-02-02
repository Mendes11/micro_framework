from collections import UserDict


DEFAULT_CONFIG = dict(
    MAX_WORKERS=3,
    WORKER_MODE='thread',
    AMPQ_URI='amqp://guest:guest@localhost:5672',
    ENABLE_METRICS=False,  # This is Preferable to avoid any port collision
    METRICS={
        'HOST': '0.0.0.0',
        'PORT': 8000,
    },
    PROCESS_WORKER_TIMEOUT=10,
    # After 10 seconds the worker will be turned off.
    WEBSOCKET_IP='0.0.0.0',
    WEBSOCKET_PORT=8765,
)


class FrameworkConfig(UserDict):
    """
    Exposes the user configurations but also return the System Default
    configurations when a config is set by the user
    """

    def _deep_update(self, config, default):
        merged_dict = default.copy()
        for name, val in config.items():
            if name in default and isinstance(val, dict):
                merged_val = self._deep_update(val, default[name])
                merged_dict[name] = merged_val
            else:
                merged_dict[name] = val
        return merged_dict

    def __init__(self, config, default_config=None):
        if default_config is None:
            default_config = DEFAULT_CONFIG
        final_config = self._deep_update(config, default_config)
        super(FrameworkConfig, self).__init__(final_config)
