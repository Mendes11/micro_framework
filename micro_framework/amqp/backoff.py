from datetime import datetime

from micro_framework.amqp.dependencies import dispatch
from micro_framework.amqp.routes import EventRoute


class EventBackOff:
    max_retries = None
    interval = 10 # ms
    exception_list = None

    def __init__(self, route):
        if not isinstance(route, EventRoute):
            raise TypeError(
                "EventBackOff can only be applied to EventRoute class."
            )
        self.route = route

    def get_interval(self):
        return self.interval

    def get_max_retries(self):
        return self.max_retries

    def get_exception_list(self):
        return self.exception_list

    def should_retry(self, payload):
        meta = payload.get('_meta', {})
        current_retries = meta.get('n_retries', 0)
        if current_retries < self.get_max_retries():
            return True
        return False

    def retry_time_is_due(self, payload):
        meta = payload.get('_meta', {})
        last_retried = meta.get('last_retried_at', 0)
        now = datetime.utcnow().timestamp()
        interval = self.get_interval()
        if last_retried + interval > now:
            return True
        return False

    def retry(self, payload):
        if self.should_retry(payload) and self.retry_time_is_due(payload):
            meta = payload.get('_meta', {})
            n_retries = meta.get('n_retries', 0) + 1
            last_retried = datetime.utcnow().timestamp()
            source_service = self.route.source_service
            event_name = self.route.event_name
            meta.update(
                {'n_retries': n_retries, 'last_retried_at': last_retried}
            )
            payload['_meta'] = meta
            dispatch(self.route.runner.config.get('AMQP_URI'),
                     source_service, event_name, payload)
