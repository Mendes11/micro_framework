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

    def bind(self, runner, parent=None):
        ext = super(Entrypoint, self).bind(runner, parent=parent)
        ext.route = parent
        return ext

    async def call_route(self, entry_id, *args, _meta=None, **kwargs):
        """
        Call the binded Route to start a new worker with the received content
        from the entrypoint.

        It returns the Instantiated Worker.

        This method caller should handle the following exceptions:

        . PoolStopped: Raises when the worker is signalled to stop.
        . Exception: Some internal error during the call to the worker.

        :param entry_id: Some object/identifier to be used to send a response
        for the correct caller.

        :param args: Additional arguments to be sent to the Route target
        function (such as payload for the event case)

        :param _meta: Metadata about this entrypoint, usually for internal
        use. (See AsyncRetry to an example)

        :param kwargs: Additional kwargs to be sent to the Route target
        function.
        """
        return await self.route.start_route(
            entry_id, *args, _meta=_meta, **kwargs
        )


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
