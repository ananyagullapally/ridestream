import sys
import os

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from surge_logic import detect_surge

def test_surge_edge_cases():
    assert detect_surge(0) == False
    assert detect_surge(14) == False
    assert detect_surge(15) == True
    assert detect_surge(100) == True
