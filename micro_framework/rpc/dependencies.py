from .proxies import RPCServiceProxy, RPCProxy


class RPCDependencyMixin:
    """
    Dependency Provider that injects a RPCProxy into the worker's target.
    """
    proxy_class: RPCServiceProxy = RPCProxy

    def get_proxy_class(self):
        return self.proxy_class

    async def get_proxy_kws(self):
        return {}

    async def get_proxy(self):
        proxy_class = self.get_proxy_class()
        kws = await self.get_proxy_kws()
        return proxy_class(**kws)

    async def get_dependency(self, worker):
        """
        Returns the RPCProxy instance with the connector that should be used.


        # NOTE: If the proxy has some synchronous connection, the user should
        run it inside the event-loop's executor.

        Simply calling:
        ```
            event_loop = asyncio.get_event_loop()
            result = await event_loop.run_in_executor(None, synchronous_task)
        ```
        """
        return await self.get_proxy()
