from unittest.mock import MagicMock, patch
import importlib


def load_module_safely():
    with patch("kafka.KafkaProducer", return_value=MagicMock()):
        module = importlib.import_module("ride_event_generator")
    return module


def test_event_schema():
    module = load_module_safely()

    event = module.generate_ride_event()

    required_fields = ["ride_id", "city", "fare", "timestamp"]

    for field in required_fields:
        assert field in event


def test_event_types():
    module = load_module_safely()

    event = module.generate_ride_event()

    assert isinstance(event["ride_id"], str)
    assert isinstance(event["city"], str)
    assert isinstance(event["fare"], (int, float))
    assert isinstance(event["timestamp"], str)
