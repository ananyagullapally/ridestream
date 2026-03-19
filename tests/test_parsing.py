import pytest
from surge_logic import detect_surge

def test_invalid_input_type():
    with pytest.raises(Exception):
        detect_surge(None)

def test_invalid_input_string():
    with pytest.raises(Exception):
        detect_surge("invalid")
