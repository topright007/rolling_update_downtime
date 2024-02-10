import logging

from RoomMeeting import *

_logger = logging.getLogger("rmsops")
def splitRoomMeetings(sameRoomRMSConnectionsLocal, meetingOnTheSameBridgeIdleTimeoutSec) -> list[RoomMeeting]:
    resultRoomMeetings = []

    for room_id, rmscInOneRoom in sameRoomRMSConnectionsLocal.items():
        resultBuckets = splitRMSCByOverlaps(rmscInOneRoom, meetingOnTheSameBridgeIdleTimeoutSec)
        for bucket in resultBuckets:
            resultRoomMeetings.append(RoomMeeting(bucket, meetingOnTheSameBridgeIdleTimeoutSec))

    return resultRoomMeetings


def registerRMSConnection(sameRoomRMSConnections: dict[str, list[RMSConnection]], rmsc: RMSConnection):
    sameRoomRMSConnections[rmsc.room_id].append(rmsc)


def loadRoomMeetings(calls, meetingOnTheSameBridgeIdleTimeoutSec):
    rmsConnections: list[RMSConnection] = [None] * calls.shape[0]
    sameRoomRMSConnections: dict[str, list[RMSConnection]] = defaultdict(list)
    errorsConnected = []
    errorsJoined = []
    errorsLeave = []
    for index in range(0, calls.shape[0]):
        try:
            rmsc = RMSConnection(
                calls['room_id'][index],
                calls['peer_id'][index],
                calls['msid'][index],
                calls['rsid'][index],
                calls['ts_joined'][index],
                calls['ts_connected'][index],
                calls['ts_leave'][index]
            )
            rmsConnections[index] = rmsc
            sameRoomRMSConnections[rmsc.room_id].append(rmsc)
        except RMSConnectionCreationException as e:
            match e.reason:
                case "joined":
                    errorsJoined.append(index)
                case "connected":
                    errorsConnected.append(index)
                case "leave":
                    errorsLeave.append(index)
                case _:
                    raise f"unknown reason {e.reason}"

    roomMeetings = splitRoomMeetings(sameRoomRMSConnections, meetingOnTheSameBridgeIdleTimeoutSec)
    _logger.info(f"Loaded {calls.shape[0]} rms into {len(roomMeetings)} room meetings. "
          f"Total errors joined: {len(errorsJoined)}, connected: {len(errorsConnected)}, leave: {len(errorsLeave)}")
    return roomMeetings
