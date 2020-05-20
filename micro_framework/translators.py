import json


class MessageTranslator:
    """
    Simple class receives a message and returns a transformation of it.
    """

    def translate(self, message):
        raise NotImplementedError()


class JSONTranslator(MessageTranslator):
    def translate(self, message):
        if isinstance(message, dict):
            return message
        return json.loads(message)