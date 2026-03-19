def detect_surge(rides_per_window, threshold=15):
    """
    Determines if surge pricing should activate.

    Surge is triggered when ride demand reaches or exceeds
    the threshold within a given time window.
    """
    return rides_per_window >= threshold
