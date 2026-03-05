def detect_surge(rides_per_window, threshold=15):
    """
    Determines if surge pricing should activate.
    """
    return rides_per_window > threshold
