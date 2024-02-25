import json
import uuid
from abc import ABC
from collections import defaultdict

from PeerConnection import *
from RMSConnection import *
from rmsutils import *


#a set of peer connections that were meeting at the same time
class RoomMeeting(ABC):
    room_id: str
    ts_start: float
    ts_finish: float
    peerConnections: list[PeerConnection]
    id: str
    meetingOnTheSameBridgeIdleTimeoutSec: int

    def __init__(self, rmsConnections: list[RMSConnection], meetingOnTheSameBridgeIdleTimeoutSec: int):
        self.room_id = rmsConnections[0].room_id
        self.ts_start = min(map(lambda conn: conn.ts_joined, rmsConnections))
        self.ts_finish = max(map(lambda conn: conn.ts_leave, rmsConnections))
        self.id = str(uuid.uuid4())
        self.meetingOnTheSameBridgeIdleTimeoutSec = meetingOnTheSameBridgeIdleTimeoutSec

        rmsByPeer = defaultdict(list)
        for conn in rmsConnections: rmsByPeer[conn.peer_id].append(conn)

        self.peerConnections = []
        for peer_id, conns in rmsByPeer.items():
            connsBuckets = splitRMSCByOverlaps(conns, meetingOnTheSameBridgeIdleTimeoutSec)
            for bucket in connsBuckets:
                self.peerConnections.append(PeerConnection(bucket, self.id))

    def __str__(self):
        return json.dumps(self, indent=4)
