import gzip
from abc import ABC


class MessageCompressor(ABC):
    content_encoding = None

    def compress(self, content: bytes) -> bytes:
        ...

    def decompress(self, content: bytes) -> bytes:
        ...


class GzipCompressor(MessageCompressor):
    content_encoding = "gzip"
    def __init__(self, compress_level=9):
        self.compress_level = compress_level

    def compress(self, content: bytes) -> bytes:
        return gzip.compress(content, compresslevel=self.compress_level)

    def decompress(self, content: bytes) -> bytes:
        return gzip.decompress(content)


_compressors = {
    "gzip": GzipCompressor
}

def compress(content: bytes, content_encoding: str) -> bytes:
    return _compressors[content_encoding]().compress(content)

def decompress(content: bytes, content_encoding: str) -> bytes:
    return _compressors[content_encoding]().decompress(content)
