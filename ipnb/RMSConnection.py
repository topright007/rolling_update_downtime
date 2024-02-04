import json
from abc import ABC
from rmsexceptions import *
from rmsutils import *


class RMSConnection(ABC):
    room_id: str
    peer_id: str
    msid: str
    rsid: str
    ts_joined: datetime
    ts_connected: datetime
    ts_leave: datetime

    def __str__(self):
        return json.dumps(self, indent=4)

    def __init__(self, room_id, peer_id, msid, rsid, ts_joined_str: str, ts_connected_str: str, ts_leave_str: str):
        self.room_id = room_id
        self.peer_id = peer_id
        self.msid = msid
        self.rsid = rsid

        if type(ts_connected_str) == float:
            raise RMSConnectionCreationException('connected')
        else:
            self.ts_connected = parseIsoDate(ts_connected_str)

        if type(ts_joined_str) == float:
            raise RMSConnectionCreationException('joined')
        else:
            self.ts_joined = parseIsoDate(ts_joined_str)

        if type(ts_leave_str) == float:
            raise RMSConnectionCreationException('leave')
        else:
            self.ts_leave = parseIsoDate(ts_leave_str)


# methods to work with RMS domain
def splitRMSCByOverlaps(rmsc: list[RMSConnection], meetingOnTheSameBridgeIdleTimeoutSec) -> list[list[RMSConnection]]:
    conns = sorted(rmsc, key=lambda conn: conn.ts_joined)
    cur_start = conns[0].ts_joined
    cur_end = conns[0].ts_leave
    cur_meeting_bucket = []
    result_meeting_buckets = []
    for conn in conns:
        # print(f"processing room {room_id}: {conn.ts_joined} - {co>=nn.ts_leave}")
        # negative values are OK here
        same_session: bool = (conn.ts_joined - cur_end).total_seconds() < meetingOnTheSameBridgeIdleTimeoutSec and (
                    cur_start - conn.ts_leave).total_seconds() < meetingOnTheSameBridgeIdleTimeoutSec
        if (same_session):
            cur_start = min(cur_start, conn.ts_joined)
            cur_end = max(cur_end, conn.ts_leave)
            cur_meeting_bucket.append(conn)
        else:
            if len(cur_meeting_bucket) > 0:
                # print(f"splitting room meeting in room {room_id}: {cur_start} - {cur_end} from {conn.ts_joined} - {conn.ts_leave}")
                result_meeting_buckets.append(cur_meeting_bucket)
            cur_start = conn.ts_joined
            cur_end = conn.ts_leave
            cur_meeting_bucket = [conn]
    if len(cur_meeting_bucket) > 0:
        result_meeting_buckets.append(cur_meeting_bucket)

    return result_meeting_buckets