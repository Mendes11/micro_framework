import pytest

from micro_framework.config import DEFAULT_CONFIG
from micro_framework.runner import Runner


@pytest.fixture
def config():
    return DEFAULT_CONFIG

