def detect_surge(rides_per_window, threshold=15):
    if not isinstance(rides_per_window, (int, float)):
        raise TypeError("rides_per_window must be a number")
    
    return rides_per_window >= threshold
