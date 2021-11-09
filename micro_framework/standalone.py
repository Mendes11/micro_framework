import asyncio
import logging
from contextlib import contextmanager
from threading import Thread

from micro_framework import Runner, Route
from micro_framework.amqp.dependencies import Producer, RPCSystemProxyProvider
from micro_framework.targets import TargetClassMethod, ClassTargetExecutor

logger = logging.getLogger(__name__)

class StandaloneExecutor(ClassTargetExecutor):
    async def after_call(self):
        return


class StandaloneTarget(TargetClassMethod):
    def get_target_executor_class(self):
        return StandaloneExecutor


class Client:
    dispatch = Producer()
    amqp_rpc = RPCSystemProxyProvider()

    def __call__(self):
        return self


async def _get_client(
        runner: Runner,
):
    await runner._start()
    worker = await runner.contexts[0].routes[0].start_route(None)
    return worker


async def _shutdown_client(runner, worker):
    await worker._on_finished_task()
    await runner._stop()


def start_loop(loop):
    try:
        loop.run_forever()
    except (asyncio.CancelledError, KeyboardInterrupt):
        return

def _shutdown(event_loop, runner, t, worker):
    asyncio.run_coroutine_threadsafe(
        _shutdown_client(runner, worker),
        event_loop
    ).result()
    event_loop.call_soon_threadsafe(event_loop.stop)

    if t:
        t.join()  # wait until completely closed.
    event_loop.close()


def _get_runner(config, worker_mode="thread"):
    runner = Runner(config=config)
    context = runner.new_context(
        worker_mode=worker_mode, inplace=True
    )
    context.add_routes(
        Route(
            Client,
            entrypoint=None,
        )
    )
    return runner


@contextmanager
def ufw_client(config):
    logger.warning(
        "THIS FEATURE IS IN BETA AND SHOULD NOT BE USED IN PRODUCTION"
    )
    logger.warning(
        "Known Issues are: "
        "* code stuck when exceptions are raised"
    )
    event_loop = asyncio.new_event_loop()
    t = Thread(target=start_loop, args=(event_loop, ), daemon=True)
    t.start()

    runner = _get_runner(config)
    worker =  asyncio.run_coroutine_threadsafe(
        _get_client(runner), event_loop
    ).result()
    if worker.exception:
        _shutdown(event_loop, runner, t, worker)
        raise worker.exception

    yield worker.result

    _shutdown(event_loop, runner, t, worker)
