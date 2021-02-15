import asyncio
import time
from contextlib import contextmanager
from functools import partial
from threading import Thread

from micro_framework.amqp.dependencies import Producer, RPCProxyProvider
from micro_framework.entrypoints import Entrypoint
from micro_framework.routes import Route
from micro_framework.runner import Runner
from micro_framework.targets import new_target
from micro_framework.workers import Worker


class StandaloneClient:
    """
    Contains a version of all Dependencies that are used to communicate with
    our entrypoints.
    """
    dispatch = Producer()
    amqp_rpc = RPCProxyProvider()

    def __call__(self, *args, **kwargs):
        return self


class StandaloneWorker(Worker):
    async def run(self, runner):
        self.runner = runner
        self.mounted_target = await self.setup_target()
        # Instead of calling the runner run and letting it handle all calls,
        # we call the pre_call and then return the class instance
        await self.mounted_target.pre_call()
        return self.mounted_target.class_instance

    async def close(self):
        # This should be called when the context manager exits.
        self.mounted_target.result = None
        self.mounted_target.exception = None
        await self.mounted_target.after_call()
        await self.finish_target()


class MicroFrameworkStandalone:
    def __init__(self, config):
        self.config = config

    async def get_client(self):
        # First we Create a runner and a RunnerContext
        self.runner = Runner(config=self.config)
        ctx = self.runner.new_context(worker_mode="asyncio", inplace=True)
        # Then we register the StandaloneTarget as a Target and register it
        # into the context by calling a Route with a mock entrypoint
        ctx.add_routes(
            Route(
                StandaloneClient, entrypoint=Entrypoint(),
                worker_class=StandaloneWorker
            )
        )

        # Start the runner to bind, setup and start all extensions
        await self.runner._start()

        # Finally, after everything is started, we call the Route and the
        # return will be the StandaloneClient itself.
        self.worker = await ctx.routes[0].get_worker_instance()
        return await self.worker.run(self.runner)

    async def stop(self):
        # Close the Worker and stops the runner.
        await self.worker.close()
        await self.runner._stop()

    async def __aenter__(self):
        return await self.get_client()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()


def _new_running_event_loop():
    """
    Instantiate a new asyncio EventLoop and then start it in a separate thread.

    # NOTE: It is important to remember that when stopping an event-loop that
    is running in a different thread, we need to call it's stop method inside
    the event-loop thread. (See _stop_event_loop function bellow)
    :return: (Running EventLoop class instance, Thread running it)
    """
    event_loop = asyncio.new_event_loop()

    t = Thread(target=event_loop.run_forever)
    t.daemon = True
    t.start()
    return event_loop, t


def _stop_event_loop(event_loop, running_thread):
    """
    Stops an event-loop that is running in a separate thread.

    It also joins that thread until everything is done.
    :param event_loop: EventLoop class instance
    :param running_thread: Thread that is running the run_forever method.
    """
    event_loop.call_soon_threadsafe(event_loop.stop)
    running_thread.join()


@contextmanager
def ufw_client(config):
    """
    Helper to get a StandaloneClient without a running event-loop.
    :param config:
    :return:
    """
    ufw_standalone = MicroFrameworkStandalone(config)
    event_loop, thread = _new_running_event_loop()

    client = asyncio.run_coroutine_threadsafe(
        ufw_standalone.get_client(), event_loop
    ).result()
    yield client

    asyncio.run_coroutine_threadsafe(ufw_standalone.stop(), event_loop).result()
    _stop_event_loop(event_loop, thread)


def async_ufw_client(config):
    """
    Return an instance of MicroFrameworkStandalone to be called as a context
    manager.

    :param config: Framework Configurations
    :return: MicroFrameworkStandalone instance
    """
    return MicroFrameworkStandalone(config)
