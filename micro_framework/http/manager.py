import asyncio
import logging
from typing import Dict

from aiohttp import web
from aiohttp.web import _run_app
from aiohttp.web_log import AccessLogger
from functools import partial

from micro_framework.entrypoints import Entrypoint
from micro_framework.extensions import Extension
from micro_framework.http.exceptions import APIError

logger = logging.getLogger(__name__)

app = web.Application()


def prepare_response(worker_response, default_error=APIError()):
    if isinstance(worker_response, web.Response):
        return worker_response

    data = default_error.data
    status = default_error.status_code

    if isinstance(worker_response, dict) or isinstance(worker_response, list):
        data = worker_response
        status = 200

    elif isinstance(worker_response, APIError):
        data = worker_response.data
        status = worker_response.status_code

    return web.json_response(data, status=status)


class HTTPManager(Extension):
    def __init__(self):
        self.entrypoints: Dict[str, Entrypoint] = {}

    async def call_entrypoint(self, entrypoint, entry_id, request, *args,
                              **kwargs):
        return await entrypoint.handle_request(
            entry_id, request, *args, **kwargs
        )

    def add_entrypoint(self, entrypoint: Entrypoint):
        """
        Expects a HTTPEntrypoint kind
        :param entrypoint:
        :return:
        """
        for method in entrypoint.methods:
            entry_id = f"{entrypoint.path}_{method}"
            target = partial(self.call_entrypoint, entrypoint, entry_id)
            app.router.add_route(
                method, entrypoint.path, target
            )
            self.entrypoints[entry_id] = entrypoint

    def setup(self):
        self.server_config = self.runner.config['HTTP_SERVER']

    async def run(self,
                  *,
                  host=None,
                  port=None,
                  path=None,
                  sock=None,
                  shutdown_timeout=60.0,
                  ssl_context=None,
                  print=logger.info,
                  backlog=128,
                  access_log_class=AccessLogger,
                  access_log_format=AccessLogger.LOG_FORMAT,
                  access_log=logger,
                  handle_signals=True,
                  reuse_address=None,
                  reuse_port=None):

        try:
            await _run_app(
                app,
                host=host,
                port=port,
                path=path,
                sock=sock,
                shutdown_timeout=shutdown_timeout,
                ssl_context=ssl_context,
                print=print,
                backlog=backlog,
                access_log_class=access_log_class,
                access_log_format=access_log_format,
                access_log=access_log,
                handle_signals=handle_signals,
                reuse_address=reuse_address,
                reuse_port=reuse_port
            )
        except asyncio.CancelledError:
            pass

    def start(self):
        self.task = self.runner.event_loop.create_task(
            self.run(**self.server_config)
        )
