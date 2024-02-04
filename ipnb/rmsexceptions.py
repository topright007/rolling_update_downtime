class RMSConnectionCreationException(Exception):
    reason: str

    def __init__(self, reason: str):
        self.reason = reason