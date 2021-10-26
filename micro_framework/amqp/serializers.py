import json
from abc import ABC, abstractmethod
from typing import Any

class MessageSerializer(ABC):
    content_type = "plain/text"

    @abstractmethod
    def serialize(self, content: Any):
        ...

    @abstractmethod
    def unserialize(self, content: bytes):
        ...


class JSONSerializer(MessageSerializer):
    content_type = "application/json"

    def serialize(self, content):
        return json.dumps(content).encode()

    def unserialize(self, content: bytes):
        return json.loads(content)

_serializers = {
    "application/json": JSONSerializer,
}

def serialize(content: Any, content_type: str) -> bytes:
    return _serializers[content_type]().serialize(content)

def unserialize(content: bytes, content_type: str) -> Any:
    return _serializers[content_type]().unserialize(content)
