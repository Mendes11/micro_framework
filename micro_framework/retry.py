import asyncio
import logging
from datetime import datetime

from kombu import Exchange, Queue

from micro_framework.amqp.connectors import PublisherClient
from micro_framework.amqp.dependencies import dispatch
from micro_framework.amqp.entrypoints import BaseEventListener
from micro_framework.dependencies import Dependency
from micro_framework.extensions import Extension
from micro_framework.routes import Route

logger = logging.getLogger(__name__)


class BackOffContext:
    """
    Object to be passed as argument to functions that implement a BackOff.
    """

    def __init__(self, retry_count, last_retry):
        self.retry_count = retry_count
        self.last_retry = last_retry


class BackOffData(Dependency):
    """
    Dependency Provider that loads the current backoff metadata from the
    worker and passes it to the user as a BackOffContext object.
    """

    def get_dependency(self, worker):
        backoff_meta = worker._meta.get('backoff', {})
        return BackOffContext(
            retry_count=backoff_meta.get('retry_count', 0),
            last_retry=backoff_meta.get('last_retry', None)
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
        self.interval = interval  # Microseconds
        self.exception_list = exception_list
        self.route = None

    async def get_interval(self, context):
        """
        Return the interval to be used on the next retry action.
        If you wish to implement custom intervals, this is the method to
        override.

        The context received is the context before updating so the
        retry_count starts with zero and the last_retry is None at first.

        :param dict context: BackOff meta data
        :return int: Interval in milliseconds.
        """
        return self.interval

    async def get_max_retries(self, context):
        """
        Return the total of retries to be compared with the current number of
        retries in order to decide to allow retry or not.
        :param dict context: BackOff meta data.
        :return int: Maximum number of retries.
        """
        return self.max_retries

    async def get_exception_list(self, context):
        """
        Returns the list of exceptions to be compared with the current worker
        exception in order to allow the retry action.

        If None is returned, then we allow all exceptions.
        :param dict context: BackOff meta data.
        :return list/None : List of Exceptions or None to allow all.
        """
        return self.exception_list

    async def _exception_in_list(self, exception, context):
        exception_list = await self.get_exception_list(context)
        if not exception_list:
            return True
        return type(exception) in exception_list

    async def _retry_count_is_lower(self, context):
        retry_count = context['retry_count']
        if retry_count < await self.get_max_retries(context):
            return True
        return False

    async def _interval_passed(self, context):
        last_retry = context['last_retry'] or 0
        if last_retry + await self.get_interval(
                context) / 1000 < datetime.utcnow().timestamp():
            return True
        return False

    async def _get_context(self, worker):
        context = worker._meta.get('backoff', None)
        if context is None:
            context = {
                'retry_count': 0, 'last_retry': None
            }
        return context

    async def can_retry(self, worker):
        """
        Check if the given worker is eligible to be sent to the retry queue.
        :param Worker worker: The resulting worker with exception
        :return bool:
        """
        backoff_context = await self._get_context(worker)
        conditions = await asyncio.gather(
            self._retry_count_is_lower(backoff_context),
            self._exception_in_list(worker.exception, backoff_context),
            self._interval_passed(backoff_context)
        )
        return all(conditions)

    async def update_context(self, context):
        # The arguments should be serializable....
        # We need to pop the context since it doesn't belong to the original
        # fn_kwargs
        retry_count = context['retry_count']
        context['retry_count'] = retry_count + 1
        context['last_retry'] = datetime.utcnow().timestamp()
        return context

    async def execute_retry(self, updated_worker, next_interval):
        """
        Hook method to implement the retry logic.

        This method should return an updated version of the worker (
        after the backoff was executed) or None for cases where the BackOff
        task will be sent to another service to handle. Ex: AsyncBackOff bellow.

        :param Worker updated_worker: A Worker with updated BackOff _meta
        :return Optional[Worker]: The Updated Worker with after backoff was
        done. If the backoff is asynchronous, returns None
        """
        raise NotImplementedError()

    async def retry(self, worker):
        """
        Called to invoke the retry action.
        It updates the backoff _meta attribute of the worker and send it
        :param Worker worker: the result worker with information to be used
        in the retry such as the fn_args and kwargs.
        """
        context = await self._get_context(worker)
        next_interval = await self.get_interval(context)
        meta = worker._meta or {}
        # Update the _meta context for the next retry before calling it
        # because of the async case.
        updated_context = await self.update_context(context)
        meta['backoff'] = updated_context
        worker._meta = meta  # meta might be a reference so this is ambiguous
        logger.info(f"{worker.target} failed, sending retry action.")
        return await self.execute_retry(worker, next_interval)

    async def bind(self, runner, parent=None):
        ext = await super(BackOff, self).bind(runner, parent=parent)
        if not isinstance(parent, Route):
            raise TypeError("A BackOff class can only be used inside a Route.")
        ext.route = parent
        return ext


class AsyncBackOff(BackOff, BaseEventListener):
    """
    BackOff that implements an Event Listener entrypoint which is responsible
    to manage the backoff of a specific route.

    Since it is an entrypoint, the binded route needs to be a copy, to enable
    us binding this entrypoint to it, instead of the original entrypoint.


    # TODO Maybe adding multiple entrypoints to a route would make this implementation cleaner
    """

    async def _interval_passed(self, context):
        return True  # RabbitMQ handles this.

    async def mount_backoff_payload(self, worker):
        return {
            'fn_args': worker.fn_args,
            'fn_kwargs': worker.fn_kwargs,
            '_meta': worker._meta
        }

    async def execute_retry(self, updated_worker, next_interval):
        # We fire the event to a queue known by the instantiated entrypoint
        retry_payload = await self.mount_backoff_payload(updated_worker)
        event_name = await self.get_event_name()
        exchange = await self.get_exchange()
        dispatch(
            self.runner.config["AMQP_URI"], exchange, event_name,
            retry_payload, headers={"x-delay": next_interval}
        )
        return None

    async def bind_route(self, route):
        self.route = route._clone()
        self.route._entrypoint = self
        await self.route.bind(self.runner)
        await self.route.entrypoint.bind_to_route(self.route)

    async def get_event_name(self):
        return f"{self.route.target}_backoff"

    async def get_exchange(self):
        """
        Return a different exchange due to the different type and arguments.
        :return Exchange:
        """
        return Exchange(
            name=self.service_name + '_backoff', type='x-delayed-message',
            arguments={'x-delayed-type': 'direct'}
        )

    async def get_queue(self):
        # TODO Multiple Routes to the same function will actually use the
        #  same queue here... is it okay?
        event_name = await self.get_event_name()
        exchange = await self.get_exchange()
        return Queue(
            name=f"{self.service_name}.{event_name}_queue",
            exchange=exchange, routing_key=event_name
        )

    async def new_entry(self, message, payload):
        # Converting the BackOff payload to the original payload and adding a
        # BackOff Context to it as kwargs
        fn_args = payload['fn_args']
        fn_kwargs = payload['fn_kwargs']
        _meta = payload['_meta']
        retry_count = _meta['backoff']['retry_count']
        max_retries = await self.get_max_retries(_meta["backoff"])
        logger.info(
            f"{self.route.target} retry {retry_count} of {max_retries}"
        )
        await self.call_route(message, *fn_args, _meta=_meta, **fn_kwargs)

    @property
    def service_name(self):
        return self.runner.config['SERVICE_NAME']
