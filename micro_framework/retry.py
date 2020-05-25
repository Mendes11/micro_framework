import logging
import time
from datetime import datetime

from kombu import Exchange, Queue

from micro_framework.amqp.dependencies import dispatch
from micro_framework.amqp.entrypoints import EventListener, BaseEventListener
from micro_framework.dependencies import Dependency
from micro_framework.entrypoints import Entrypoint
from micro_framework.exceptions import BackOffLimitReached, \
    BackOffIntervalNotReached
from micro_framework.extensions import Extension


logger = logging.getLogger(__name__)


class BackOffContext:
    """
    Object to be passed as argument to functions that implement a BackOff.
    """
    def __init__(self, retry_count, last_retry):
        self.retry_count = retry_count
        self.last_retry = last_retry


class BackOffData(Dependency):
    def get_dependency(self, worker):
        backoff_meta = worker._meta.get('backoff', {})
        return BackOffContext(
            retry_count=backoff_meta.get('retry_count', 0),
            last_retry=backoff_meta.get('last_retry', 0)
        )


class BackOff(Extension):
    """
    Adds BackOff Capability to a failed route. It is responsible to firing
    retries when needed.
    """
    def __init__(self, max_retries=3, interval=60000, exception_list=None):
        """
        :param int max_retries: Total number of retry attempts.
        :param interval: Interval between retries in micro seconds
        :param exception_list: list of exceptions that enable retries.
        """
        self.max_retries = max_retries
        self.interval = interval # Microseconds
        self.exception_list = exception_list
        self.route = None

    def get_interval(self):
        return self.interval

    def get_max_retries(self):
        return self.max_retries

    def get_exception_list(self):
        return self.exception_list

    def exception_in_list(self, exception):
        if not self.exception_list:
            return True
        return type(exception) in self.exception_list

    def retry_count_is_lower(self, context):
        retry_count = 0
        if context:
            retry_count = context['retry_count']
        if retry_count < self.get_max_retries():
            return True
        return False

    def interval_passed(self, context):
        last_retry = 0
        if context:
            last_retry = context['last_retry']
        if last_retry + self.interval / 1000 < datetime.utcnow().timestamp():
            return True
        return False

    def get_context(self, worker):
        return worker._meta.get('backoff', None)

    def can_retry(self, worker):
        """
        Check if the given worker is eligible to be sent to the retry queue.
        :param Worker worker: The resulting worker with exception
        :return bool:
        """
        backoff_context = self.get_context(worker)
        return all(
            [self.retry_count_is_lower(backoff_context),
             self.exception_in_list(worker.exception),
             self.interval_passed(backoff_context)
             ]
        )

    def update_meta(self, worker):
        # The arguments should be serializable....
        # We need to pop the context since it doesn't belong to the original
        # fn_kwargs
        context = self.get_context(worker)
        retry_count = 1
        if context is not None:
            retry_count = context['retry_count'] + 1
        meta = worker._meta or {}
        updated_backoff = {
            'retry_count': retry_count,
            'last_retry': datetime.utcnow().timestamp()
        }
        meta.setdefault('backoff', {}).update(updated_backoff)
        return meta

    def execute_retry(self, updated_worker):
        """
        Hook method to implement the retry logic. Ex: AsyncBackOff bellow.

        :param Worker updated_worker: A Worker with updated BackOff _meta
        """
        raise NotImplementedError()

    def retry(self, worker):
        """
        Called to invoke the retry action.
        It updates the backoff _meta attribute of the worker and send it
        :param Worker worker: the result worker with information to be used
        in the retry such as the fn_args and kwargs.
        """
        updated_meta = self.update_meta(worker)
        worker._meta = updated_meta
        logger.info(f"{worker.function_path} failed, sending retry "
                    f"{updated_meta['backoff']['retry_count']} of "
                    f"{self.max_retries}")
        self.execute_retry(worker)

    def bind_route(self, route):
        self.route = route


class AsyncBackOff(BackOff, BaseEventListener):
    """
    BackOff that implements an Event Listener entrypoint which is responsible
    to manage the backoff of a specific route.

    Since it is an entrypoint, the binded route needs to be a copy, to enable
    us binding this entrypoint to it, instead of the original entrypoint.


    # TODO Maybe adding multiple entrypoints to a route would make this implementation cleaner
    """
    def interval_passed(self, context):
        return True # RabbitMQ handles this.

    def mount_backoff_payload(self, worker):
        return {
            'fn_args': worker.fn_args,
            'fn_kwargs': worker.fn_kwargs,
            '_meta': worker._meta
        }

    def execute_retry(self, updated_worker):
        # We fire the event to a queue known by the instantiated entrypoint
        retry_payload = self.mount_backoff_payload(updated_worker)
        dispatch(
            self.runner.config['AMQP_URI'],
            self.service_name,
            self.get_event_name(),
            retry_payload,
            exchange=self.get_exchange(),
            headers={'x-delay': self.get_interval()}
        )

    def bind_route(self, route):
        self.route = route._clone()
        self.route._entrypoint = self
        self.route.bind(self.runner)
        self.route.entrypoint.bind_to_route(self.route)

    def get_event_name(self):
        return f"{self.route.function_path}_backoff"

    def get_exchange(self):
        return Exchange(
            name=self.service_name+'_backoff', type='x-delayed-message',
            arguments={'x-delayed-type': 'direct'}
        )

    def get_queue(self):
        return Queue(
            name=f"{self.get_event_name()}_queue",
            exchange=self.get_exchange(), routing_key=self.get_event_name()
        )

    def new_entry(self, message, payload):
        # Converting the BackOff payload to the original payload and adding a
        # BackOff Context to it as kwargs
        fn_args = payload['fn_args']
        fn_kwargs = payload['fn_kwargs']
        _meta = payload['_meta']
        self.call_route(message, *fn_args, _meta=_meta, **fn_kwargs)

    @property
    def service_name(self):
        return self.runner.config['SERVICE_NAME']
