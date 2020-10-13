import json

from aiohttp import web

from micro_framework.http.exceptions import APIError


class Renderer:
    def render_response(self, response):
        if isinstance(response, dict) or isinstance(response, list):
            response = json.dumps(response)

        return web.Response(text=response)


class JsonRenderer:
    def render_response(self, response):
        if isinstance(response, web.Response):
            return response

        data = response
        status = 200

        if isinstance(response, APIError):
            data = response.data
            status = response.status_code

        return web.json_response(data, status=status)
