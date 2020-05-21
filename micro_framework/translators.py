import json


class MessageTranslator:
    """
    Simple class receives a message and returns a transformation of it.
    """
    def get_message(self, *args, **kwargs):
        return args[0]

    def set_message(self, message, *args, **kwargs):
        list(args)[0] = message
        return args, kwargs

    def translate_message(self, message):
        raise NotImplementedError()

    def translate(self, *args, **kwargs):
        message = self.get_message(*args, **kwargs)
        translated = self.translate(message)
        return self.set_message(translated, *args, **kwargs)


class JSONTranslator(MessageTranslator):
    def translate_message(self, message):
        if isinstance(message, dict):
            return message
        return json.loads(message)
