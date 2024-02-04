from abc import ABC
from dataclasses import dataclass
from enum import Enum

from rmsops import *
from RoomMeetingAssignments import *
from policies import *


@dataclass
class RMSRestartResult:
    totalDTSec: int
    downtimeStart: datetime
    downtimeFinish: datetime


class RMSRestarter(ABC):
    assignments: RoomMeetingAssignments

    startDowntimeAt: datetime
    newNodePolicy: NewNodePolicy
    meetings: list[RoomMeeting]

    ts: datetime
    curMeetingIdx: int
    # node id -> time until maintenance finishes
    nodesInMaintenance: dict[int, datetime]

    def __init__(self, meetings: list[RoomMeeting], startDowntimeAt: datetime, policy: NewNodePolicy):
        self.meetings = meetings
        self.startDowntimeAt = startDowntimeAt
        self.newNodePolicy = policy

        self.assignments = RoomMeetingAssignments()
        self.ts = meetings[0].ts_start
        self.nextMeetingIdx = 0

    def measureDT(self) -> RMSRestartResult:
        # order meetings by start date and by end date
        # iterate meeting starts, meeting finishes and node maintenances by ts: assign and unassign meetings
        # (complexity: 2*M*log(M) to sort + 2*M*DISR_BUDGET to merge lists by ts with maintenances )
        #
        # when maintenance starts, close nodes for maintenance
        # when maintenance ends, assign all their unfinished meetings to a new node and calculate downtime by number of participants
        return RMSRestartResult(0, datetime.now(), datetime.now())
