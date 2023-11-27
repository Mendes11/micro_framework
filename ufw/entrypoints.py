from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ufw.ufw import UFW

class Entrypoint:
    """
    An entrypoint holds the logic to receive messages from external systems
    Its responsibility is to notify the application whenever a message is
    received
    """

    async def setup(self, app: 'UFW'):
        ...

    async def start(self):
        ...


class TimerEntrypoint(Entrypoint):
    async def setup(self, app: 'UFW'):
        self.app = app

    async def start(self):
        while self.app.running:
            await self.app.new_message(self, datetime.utcnow())
