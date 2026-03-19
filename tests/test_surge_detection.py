import pytest
from surge_logic import detect_surge


def test_surge_edge_cases():
    assert detect_surge(0) is False
    assert detect_surge(14) is False
    assert detect_surge(15) is True
    assert detect_surge(100) is True


def test_invalid_input_type():
    with pytest.raises(TypeError):
        detect_surge(None)
