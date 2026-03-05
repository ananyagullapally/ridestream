import sys
import os

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from surge_logic import detect_surge


def test_surge_true():
    assert detect_surge(20) == True


def test_surge_false():
    assert detect_surge(5) == False


def test_surge_threshold_edge():
    assert detect_surge(15) == False
