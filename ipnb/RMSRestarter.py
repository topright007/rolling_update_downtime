import sys
from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import Callable

from rmsops import *
from RoomMeetingAssignments import *
from policies import *


@dataclass
class RMSRestartResult:
    totalDTSec: int
    downtimeStart: datetime
    downtimeFinish: datetime


@dataclass
class RMSRestarterEvent:
    ts: datetime
    action: Callable[[], None]


@dataclass
class NodeMaintenanceRecord:
    ts: datetime
    node: int


class RMSRestarter(ABC):
    assignments: RoomMeetingAssignments

    # when downtimes are planned to start (i.e. new version is being rolled out)
    # each new downtime will cancel previous rollout
    startRolloutAt: list[datetime]
    disruptionBudget: int
    newNodePolicy: NewNodePolicy
    meetings: list[RoomMeeting]

    ts: datetime
    curMeetingIdx: int
    # events to finish maintenance are generated when maintenance of a node is started
    finishMaintenanceEvents: list[RMSRestarterEvent] = []

    def __init__(self, meetings: list[RoomMeeting], startRolloutAt: list[datetime], disruptionBudget: int,
                 policy: NewNodePolicy):
        self.meetings = meetings
        self.startRolloutAt = startRolloutAt
        self.disruptionBudget = disruptionBudget
        self.newNodePolicy = policy

        self.assignments = RoomMeetingAssignments()
        self.ts = meetings[0].ts_start
        self.nextMeetingIdx = 0

    def meetingStarted(self, rm: RoomMeeting):
        pass

    def meetingFinished(self, rm: RoomMeeting):
        pass

    def nodeMaintenanceStarted(self, nodeId: int):
        pass

    def nodeMaintenanceFinished(self, nodeId: int):
        pass

    def startRollout(self, ts: datetime):
        pass

    @staticmethod
    def lastTsReached(restartEventLists: list[list[RMSRestarterEvent]], restartEventListIndexes: list[int]):
        assert len(restartEventLists) == len(restartEventListIndexes), "length of both lists expected to be equal"
        for i in range(0, len(restartEventListIndexes)):
            if restartEventListIndexes[i] < len(restartEventLists[i]):
                return False
        return True

    def measureDT(self) -> RMSRestartResult:
        # order meetings by start date and by end date
        # iterate meeting starts, meeting finishes and node maintenances by ts: assign and unassign meetings
        # (complexity: 2*M*log(M) to sort + 2*M*DISR_BUDGET to merge lists by ts with maintenances )
        #
        # when maintenance starts, close nodes for maintenance
        # when maintenance ends, assign all their unfinished meetings to a new node and calculate downtime by number of participants

        meetingByStartTs = self.meetings.copy()
        meetingByStartTs.sort(key=lambda k: k.ts_start)

        meetingByFinishTs = self.meetings.copy()
        meetingByFinishTs.sort(key=lambda k: k.ts_finish)

        meetingStartEvents: list[RMSRestarterEvent] = list(map(
            lambda meeting: RMSRestarterEvent(meeting.ts_start, lambda meeting: self.meetingStarted(meeting)),
            meetingByStartTs
        ))
        meetingFinishEvents: list[RMSRestarterEvent] = list(map(
            lambda meeting: RMSRestarterEvent(meeting.ts_finish, lambda meeting: self.meetingStarted(meeting)),
            meetingByFinishTs
        ))

        startRolloutEvents: list[RMSRestarterEvent] = list(map(
            lambda rolloutTs: RMSRestarterEvent(rolloutTs, lambda ts: self.startRollout(rolloutTs)),
            self.startRolloutAt
        ))

        restartEventLists: list[list[RMSRestarterEvent]] = [
            meetingStartEvents,
            meetingFinishEvents,
            startRolloutEvents,
            self.finishMaintenanceEvents
        ]

        restartEventListIndexes: list[int] = [0] * len(restartEventLists)
        restartEventListLimits: list[int] = list(map(lambda theList:len(theList), restartEventLists))

        # when last meeting finishes, there's no downtime
        while not self.lastTsReached(restartEventLists, restartEventListIndexes):
            minTs:datetime = None
            minIndex: int = None
            for i in range(0, len(restartEventLists)):
                #no more events of this type for now
                if restartEventListIndexes[i] >= restartEventListLimits[i]:
                    continue
                eventTs = restartEventLists[i][restartEventListIndexes[i]].ts
                if minTs is None or eventTs < minTs:
                    minTs = eventTs
                    minIndex = i

            # do the action that is next on the time scale and shift the pointer
            if minIndex is not None:
                restartEventLists[minIndex][restartEventListIndexes[minIndex]].action()
                restartEventListIndexes[minIndex] += 1


        return RMSRestartResult(0, datetime.now(), datetime.now())
