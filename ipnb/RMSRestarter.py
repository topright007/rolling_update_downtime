from datetime import datetime, timedelta
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
class NodeMaintenanceRecord:
    ts: datetime
    node: int


@dataclass
class RMSRolloutMeetingDowntime(ABC):
    ts: datetime
    rm: RoomMeeting


class RMSRollout(ABC):
    startTs: datetime
    #datetime - meeting_id -> num_participants
    downtimes: list[RMSRolloutMeetingDowntime]

    def __init__(self, startTs: datetime):
        self.downtimes = []
        self.startTs = startTs

    def downtime(self, ts: datetime, rm: RoomMeeting):
        self.downtimes.append(RMSRolloutMeetingDowntime(ts, rm))


@dataclass
class RMSRestarterEvent:
    ts: datetime
    action: Callable[[], None]


class RMSFinishGraceEvent (RMSRestarterEvent):
    nodeId: int
    rollout: RMSRollout

    def __init__(self, ts: datetime, action: Callable[[], None], nodeId: int, rollout: RMSRollout):
        super().__init__(ts, action)
        self.nodeId = nodeId
        self.rollout = rollout


class MultiListTimestampTraverser:
    restartEventLists: list[list[RMSRestarterEvent]]
    restartEventListIndexes: list[int]

    def __init__(self, restartEventLists: list[list[RMSRestarterEvent]]):
        self.restartEventLists = restartEventLists
        self.restartEventListIndexes = [0] * len(restartEventLists)

    def lastTsReached(self):
        for i in range(0, len(self.restartEventListIndexes)):
            if self.restartEventListIndexes[i] < len(self.restartEventLists[i]):
                return False
        return True

    def traverse(self):
        while not self.lastTsReached():
            minTs: datetime = None
            minIndex: int = None
            for i in range(0, len(self.restartEventLists)):
                # no more events of this type for now
                currentList = self.restartEventLists[i]
                currentListIndex = self.restartEventListIndexes[i]
                currentListSize = len(currentList)
                if currentListIndex >= currentListSize:
                    continue
                eventTs = currentList[currentListIndex].ts
                if minTs is None or eventTs < minTs:
                    minTs = eventTs
                    minIndex = i

            # do the action that is next on the timescale and shift the pointer
            if minIndex is not None:
                self.restartEventLists[minIndex][self.restartEventListIndexes[minIndex]].action()
                self.restartEventListIndexes[minIndex] += 1


class RMSRestarter(ABC):
    assignments: RoomMeetingAssignments

    # when downtimes are planned to start (i.e. new version is being rolled out)
    # each new downtime will cancel previous rollout
    startRolloutAt: list[datetime]
    disruptionBudget: int
    nodeRestartsInSec: int
    shardsConfig: ShardsConfig
    newNodePolicy: NewNodePolicy
    meetings: list[RoomMeeting]

    # events to finish maintenance are generated when maintenance of a node is started
    finishGraceEvents: list[RMSFinishGraceEvent]
    #nodeId -> index in finishMaintenanceEvents
    nodesInGraceIndex: dict[int, int]

    nodesStartupEvents: list[RMSRestarterEvent]

    nextNodeToRollout: int

    rollouts: list[RMSRollout]

    def __init__(self, meetings: list[RoomMeeting], startRolloutAt: list[datetime], disruptionBudget: int, nodeRestartsInSec: int,
                 shardsConfig: ShardsConfig, policy: NewNodePolicy):
        self.meetings = meetings
        self.startRolloutAt = startRolloutAt
        self.disruptionBudget = disruptionBudget
        self.nodeRestartsInSec = nodeRestartsInSec
        self.shardsConfig = shardsConfig
        self.newNodePolicy = policy

        self.assignments = RoomMeetingAssignments()
        self.finishGraceEvents = []
        self.nodesInGraceIndex = {}
        self.nodesStartupEvents = []
        self.rollouts = []

    def meetingStarted(self, rm: RoomMeeting):
        node = self.newNodePolicy.pickNodeForRoom(rm.ts_start, self.assignments)
        self.assignments.assignRoomMeeting(rm, node, rm.ts_start)
        print(f"{formatIsoDate(rm.ts_start)}: meeting started  {rm.id} on node {node}")

    def meetingFinished(self, rm: RoomMeeting):
        currentNode = self.assignments.getCurrentNode(rm, rm.ts_finish)
        self.assignments.releaseRoomMeeting(rm, rm.ts_finish)
        print(f"{formatIsoDate(rm.ts_finish)}: meeting finished {rm.id} on node {currentNode}")
        self.tryFinishMaintenanceIfNoMoreMeetings(currentNode, rm.ts_finish)

    def scheduleFinishGrace(self, nodeId: int, graceFinishesTs: datetime, rollout: RMSRollout):
        self.finishGraceEvents.append(RMSFinishGraceEvent(
            graceFinishesTs,
            lambda: self.nodeGraceFinished(nodeId, graceFinishesTs, rollout),
            nodeId,
            rollout
        ))
        self.nodesInGraceIndex[nodeId] = len(self.finishGraceEvents) - 1

    # check if there are no more meetings on the node and schedule restart now if applicable
    # this method will sink the event to the current ts in the list of fininsh grace events.
    # It will be picked up by the next loop
    def tryFinishMaintenanceIfNoMoreMeetings(self, nodeId: int, ts: datetime):
        # node was not in grace
        if not self.assignments.isNodeInMaintenance(nodeId, ts):
            return

        # node still has meetings. grace continues
        if self.assignments.nodeHasMeetings(nodeId, ts):
            return

        finishGraceEventIndex = self.nodesInGraceIndex[nodeId]
        graceFinishEvent = self.finishGraceEvents[finishGraceEventIndex]
        assert graceFinishEvent.nodeId == nodeId, \
            f"bug found: node id in maintenance finish index node {nodeId} " \
            f"does't correspond to the actual node ID in the event node {graceFinishEvent.nodeId}"

        newFinishGraceEventIndexIndex: int = 0
        for i in range(finishGraceEventIndex-1, -1, -1):
            # shift all events that should start after our new maintenance time to +1
            if self.finishGraceEvents[i].ts > ts:
                evt = self.finishGraceEvents[i]
                newIndex = i+1
                self.finishGraceEvents[newIndex] = evt
                self.nodesInGraceIndex[evt.nodeId] = newIndex
                newFinishGraceEventIndexIndex = i
            else:
                break

        print(f"maintenance of node {nodeId} shifted from {formatIsoDate(graceFinishEvent.ts)} "
              f"to {formatIsoDate(ts)} because there were no meetings")
        # register the new event in the correct index and with a correct ts
        assert nodeId == graceFinishEvent.nodeId, f"bug found: nodeId {nodeId} does not correspoint to event node id {graceFinishEvent.nodeId}"
        # reinit the event because we can't change lambda
        graceFinishEvent = RMSFinishGraceEvent(
            ts,
            lambda: self.nodeGraceFinished(nodeId, ts, graceFinishEvent.rollout),
            nodeId,
            graceFinishEvent.rollout
        )
        self.finishGraceEvents[newFinishGraceEventIndexIndex] = graceFinishEvent
        self.nodesInGraceIndex[nodeId] = newFinishGraceEventIndexIndex

        if newFinishGraceEventIndexIndex > 0:
            assert self.finishGraceEvents[newFinishGraceEventIndexIndex-1].ts <= self.finishGraceEvents[newFinishGraceEventIndexIndex].ts

        if newFinishGraceEventIndexIndex < len(self.finishGraceEvents)-1:
            assert self.finishGraceEvents[newFinishGraceEventIndexIndex+1].ts >= self.finishGraceEvents[newFinishGraceEventIndexIndex].ts

    def returnNodeToDuty(self, nodeId: int, ts: datetime):
        print(f"{formatIsoDate(ts)}: returning node {nodeId} to duty")

        self.assignments.endNodeMaintenance(nodeId, ts)
        if nodeId in self.nodesInGraceIndex:
            self.nodesInGraceIndex.pop(nodeId)

        self.disruptNodes(ts)

    def scheduleNodeStartup(self, nodeId: int, startupStarts: datetime):
        print(f"{formatIsoDate(startupStarts)}: started startup of node {nodeId}")
        startupFinishes = startupStarts + timedelta(seconds=self.nodeRestartsInSec)
        self.nodesStartupEvents.append(
            RMSRestarterEvent(startupFinishes, lambda: self.returnNodeToDuty(nodeId, startupFinishes))
        )


    def nodeGraceStarted(self, nodeId: int, ts: datetime):
        gracePeriod: timedelta

        if self.assignments.nodeHasMeetings(nodeId, ts):
            # if node has active meetings, it needs to wait for grace period, and then it will restart
            gracePeriod = timedelta(seconds=float(self.newNodePolicy.gracePeriod(ts, self.assignments)))
            downtimeFinishes = ts + gracePeriod
            self.scheduleFinishGrace(nodeId, downtimeFinishes, self.rollouts[-1])
            print(f"{formatIsoDate(ts)}: started grace period of node {nodeId}")
        else:
            print(f"{formatIsoDate(ts)}: grace not needed for node {nodeId}")
            self.scheduleNodeStartup(nodeId, ts)

        self.assignments.startNodeMaintenance(nodeId, ts)

    def disruptNodes(self, ts: datetime):
        while len(self.assignments.getNodesInMaintenance(ts)) < self.disruptionBudget and \
                self.nextNodeToRollout < self.shardsConfig.numNodesGlobal():
            self.nodeGraceStarted(self.nextNodeToRollout, ts)
            self.nextNodeToRollout += 1

    def nodeGraceFinished(self, nodeId: int, ts: datetime, rollout: RMSRollout):
        meetingsLeft = self.assignments.getNodeMeetings(nodeId, ts)
        print(f"{formatIsoDate(ts)}: finished grace period of node {nodeId}. active meetings: {meetingsLeft}")
        for meetingId in meetingsLeft:
            rm = self.assignments.roomMeetingById(meetingId)
            newNodeId = self.newNodePolicy.pickNodeForRoom(ts, self.assignments)
            self.assignments.assignRoomMeeting(rm, newNodeId, ts)
            rollout.downtime(ts, rm)
            print(f"{formatIsoDate(ts)}: reassigning room meeting {meetingId} from node {nodeId} to node {newNodeId}")

        self.scheduleNodeStartup(nodeId, ts)
        #todo: increase dt for those meetings that are still on the node. reassign meetings


    def startRollout(self, ts: datetime):
        #todo start multiple rollouts. register new downtime report for each
        #todo downtime reports should account for all graces started during the downtime. Even if grace finishes during another downtime
        print(f"{formatIsoDate(ts)}: starting rollout. ")
        self.rollouts.append(RMSRollout(startTs=ts))

        self.nextNodeToRollout = 0
        self.disruptNodes(ts)

    @staticmethod
    def lastTsReached(restartEventLists: list[list[RMSRestarterEvent]], restartEventListIndexes: list[int]):
        assert len(restartEventLists) == len(restartEventListIndexes), "length of both lists expected to be equal"
        for i in range(0, len(restartEventListIndexes)):
            if restartEventListIndexes[i] < len(restartEventLists[i]):
                return False
        return True

    def measureDT(self) -> list[RMSRestartResult]:
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
            lambda meeting: RMSRestarterEvent(meeting.ts_start, lambda: self.meetingStarted(meeting)),
            meetingByStartTs
        ))
        meetingFinishEvents: list[RMSRestarterEvent] = list(map(
            lambda meeting: RMSRestarterEvent(meeting.ts_finish, lambda: self.meetingFinished(meeting)),
            meetingByFinishTs
        ))

        startRolloutEvents: list[RMSRestarterEvent] = list(map(
            lambda rolloutTs: RMSRestarterEvent(rolloutTs, lambda: self.startRollout(rolloutTs)),
            self.startRolloutAt
        ))

        restartEventLists: list[list[RMSRestarterEvent]] = [
            meetingStartEvents,
            meetingFinishEvents,
            startRolloutEvents,
            self.finishGraceEvents,
            self.nodesStartupEvents
        ]

        traverser = MultiListTimestampTraverser(restartEventLists)
        traverser.traverse()

        return []
