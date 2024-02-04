from abc import ABC
from datetime import datetime

from RMSConnection import RMSConnection


class PeerConnection(ABC):
    room_id: str
    peer_id: str
    ts_joined: datetime
    ts_connected: datetime
    ts_leave: datetime
    rmsConnections: list[RMSConnection]

    def __init__(self, rmsConnections: list[RMSConnection]):
        self.room_id = rmsConnections[0].room_id
        self.peer_id = rmsConnections[0].peer_id
        self.ts_joined = min(map(lambda conn: conn.ts_joined, rmsConnections))
        self.ts_connected = min(map(lambda conn: conn.ts_connected, rmsConnections))
        self.ts_leave = max(map(lambda conn: conn.ts_leave, rmsConnections))
        self.rmsConnections = rmsConnections

    def __str__(self):
        connectionsStr = ''
        for rmsc in self.rmsConnections:
            connectionsStr += f"{rmsc.ts_joined} - {rmsc.ts_leave},"

        return f"room_id: {self.room_id}; peer_id: {self.peer_id}; ts_joined: {self.ts_joined}; ts_connected: {self.ts_connected}; ts_leave: {self.ts_leave}; rmsConnections: {connectionsStr}"
