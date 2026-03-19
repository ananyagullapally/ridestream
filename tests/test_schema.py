import pytest
from unittest.mock import MagicMock, patch
import importlib


@pytest.fixture
def event():
    with patch("kafka.KafkaProducer", return_value=MagicMock()):
        module = importlib.import_module("ride_event_generator")
    return module.generate_ride_event()


def test_event_schema(event):
    for field in ["ride_id", "city", "fare", "timestamp"]:
        assert field in event


def test_event_types(event):
    assert isinstance(event["ride_id"], str)
    assert isinstance(event["city"], str)
    assert isinstance(event["fare"], (int, float))
    assert isinstance(event["timestamp"], str)
