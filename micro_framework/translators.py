import json


class MessageTranslator:
    """
    Simple class receives a message and returns a transformation of it.
    """
    async def get_message(self, *args, **kwargs):
        return args[0]

    async def set_message(self, message, *args, **kwargs):
        list(args)[0] = message
        return args, kwargs

    async def translate_message(self, message):
        raise NotImplementedError()

    async def translate(self, *args, **kwargs):
        message = await self.get_message(*args, **kwargs)
        translated = await self.translate_message(message)
        return await self.set_message(translated, *args, **kwargs)


class JSONTranslator(MessageTranslator):
    async def translate_message(self, message):
        if isinstance(message, dict):
            return message
        return json.loads(message)
