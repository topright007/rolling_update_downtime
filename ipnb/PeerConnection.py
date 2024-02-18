from abc import ABC

from RMSConnection import RMSConnection
from rmsutils import formatIsoDate


class PeerConnection(ABC):
    room_id: str
    peer_id: str
    ts_joined: float
    ts_connected: float
    ts_leave: float
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

        return f"room_id: {self.room_id}; peer_id: {self.peer_id}; ts_joined: {formatIsoDate(self.ts_joined)}; ts_connected: {formatIsoDate(self.ts_connected)}; ts_leave: {formatIsoDate(self.ts_leave)}; rmsConnections: {connectionsStr}"
