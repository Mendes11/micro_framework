import asyncio
import logging
import uuid
from datetime import datetime
from timeit import default_timer as timer

from micro_framework.exceptions import PoolStopped, ExtensionIsStopped
from micro_framework.extensions import Extension
from micro_framework.routes import Route

logger = logging.getLogger(__name__)


class Entrypoint(Extension):
    route = None

    async def bind(self, runner, parent=None):
        ext = await super(Entrypoint, self).bind(runner, parent=parent)
        if ext != self and not isinstance(parent, Route):
            raise TypeError(
                f"An Entrypoint can only be used inside a Route. "
                f"{type(parent)} was used"
            )
        ext.route = parent
        return ext

    async def call_route(self, entry_id, *args, _meta=None, **kwargs):
        """
        Call the binded Route to start a new worker with the received content
        from the entrypoint.

        It returns the Instantiated Worker.

        :param entry_id: Some object/identifier to be used to send a response
        for the correct caller.
        :param args: Additional arguments to be sent to the Route target
        function (such as payload for the event case)
        :param _meta: Metadata about this entrypoint, usually for internal
        use. (See AsyncRetry to an example)
        :param kwargs: Additional kwargs to be sent to the Route target
        function.
        """
        try:
            worker = await self.route.start_route(
                entry_id, *args, _meta=_meta, **kwargs
            )
            await self.on_finished_route(entry_id, worker)
        except PoolStopped:  # Unexpected error and we want to
            logger.info("New entry called when pool was already "
                        "signalled to stop.")
            await self.on_failure(entry_id)
        except Exception:
            logger.exception("Failure when trying to start route for "
                             f"entrypoint: {self}")

    async def on_finished_route(self, entry_id, worker):
        """
        After a route is finished, this method is called with the finished
        worker.

        The worker will have a result and exception attributes, to check if
        it was successful or not.

        Also note that if some asynchronous backoff is used
        (ie: the RabbitMQ retry plugin implemented in this Framework)

        The worker might be returned as None since the route called the retry
        method and then we should ignore the worker result.


        :param entry_id: Some object/identifier to be used to send a response
        for the correct caller.
        :param Optional[Worker] worker: Worker Instance with result/exception
        attrs or a None type, indicating an asynchronous backoff was made.
        """
        pass

    async def on_failure(self, entry_id):
        """
        When a Route fails to spawn a worker to some reason, it will call
        this method to handle what to do about it.

        :param entry_id: Some object/identifier to be used to send a response
        for the correct caller.
        """
        pass


class TimeEntrypoint(Entrypoint):
    def __init__(self, interval):
        self.interval = interval
        self.running = False

    async def start(self):
        """
        Start a loop that sleeps for a configured interval and then call the
        bind Route.

        Since it runs in a loop, some caution should be taken to prevent it
        from locking the event loop.
        """
        self.running = True
        last_time = timer()

        while self.running:
            elapsed_time = timer() - last_time
            sleep_time = self.interval - elapsed_time
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            last_time = timer()
            entry_id = uuid.uuid4()
            now = datetime.utcnow()
            try:
                await self.call_route(entry_id, now.isoformat())
            except (ExtensionIsStopped, PoolStopped):
                pass

    async def stop(self):
        self.running = False
