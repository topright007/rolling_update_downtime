from RoomMeeting import *


def splitRoomMeetings(sameRoomRMSConnectionsLocal, meetingOnTheSameBridgeIdleTimeoutSec) -> list[RoomMeeting]:
    resultRoomMeetings = []

    for room_id, rmscInOneRoom in sameRoomRMSConnectionsLocal.items():
        resultBuckets = splitRMSCByOverlaps(rmscInOneRoom, meetingOnTheSameBridgeIdleTimeoutSec)
        for bucket in resultBuckets:
            resultRoomMeetings.append(RoomMeeting(bucket, meetingOnTheSameBridgeIdleTimeoutSec))

    return resultRoomMeetings