from datetime import datetime, timedelta
import sys
from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import Callable

from rmsops import *
from RoomMeetingAssignments import *
from policies import *

_logger = logging.getLogger("RMSRestarter")

@dataclass
class NodeMaintenanceRecord:
    ts: float
    node: int


@dataclass
class RMSRolloutMeetingDowntime(ABC):
    ts: float
    rm: RoomMeeting


class RMSRollout(ABC):
    startTs: float
    finishTs: float
    #ts - meeting_id -> num_participants
    downtimes: list[RMSRolloutMeetingDowntime]

    def __init__(self, startTs: float):
        self.downtimes = []
        self.startTs = startTs

    def downtime(self, ts: float, rm: RoomMeeting):
        self.downtimes.append(RMSRolloutMeetingDowntime(ts, rm))

    def finish(self, ts: float):
        self.finishTs = ts


@dataclass
class RMSRestarterEvent:
    ts: float
    action: Callable[[], None]


class RMSFinishGraceEvent (RMSRestarterEvent):
    nodeId: int
    rollout: RMSRollout

    def __init__(self, ts: float, action: Callable[[], None], nodeId: int, rollout: RMSRollout):
        super().__init__(ts, action)
        self.nodeId = nodeId
        self.rollout = rollout


class MultiListTimestampTraverser:
    restartEventLists: list[list[RMSRestarterEvent]]
    restartEventListIndexes: list[int]
    lastReportedRealTs: datetime | None

    def __init__(self, restartEventLists: list[list[RMSRestarterEvent]]):
        self.restartEventLists = restartEventLists
        self.restartEventListIndexes = [0] * len(restartEventLists)
        self.lastReportedRealTs = None

    def lastTsReached(self):
        for i in range(0, len(self.restartEventListIndexes)):
            if self.restartEventListIndexes[i] < len(self.restartEventLists[i]):
                return False
        return True

    def traverse(self):
        totalEvents = sum(map(lambda l: len(l), self.restartEventLists))
        _logger.info(f"Started traversal of events. Total number of events is {totalEvents}")
        eventsCnt = 0
        while not self.lastTsReached():
            minTs: float | None = None
            minIndex: int | None = None
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
                if self.lastReportedRealTs is None or (datetime.now() - self.lastReportedRealTs).total_seconds() > 5:
                    self.lastReportedRealTs = datetime.now()
                    _logger.info(f"Events processed: {int(float(eventsCnt)/float(totalEvents)*100.0)}%. Date: {formatIsoDate(self.restartEventLists[minIndex][self.restartEventListIndexes[minIndex]].ts)}. Current event: {eventsCnt}")
                self.restartEventLists[minIndex][self.restartEventListIndexes[minIndex]].action()
                self.restartEventListIndexes[minIndex] += 1
                eventsCnt += 1
        _logger.info(f"Finished traversal of events")


class RMSSortedMeetings(ABC):
    meetingByStartTs: list[RoomMeeting]
    meetingByFinishTs: list[RoomMeeting]
    pcByConnectTs: list[PeerConnection]
    pcByLeaveTs: list[PeerConnection]

    def __init__(self, roomMeetings: list[RoomMeeting]):
        self.meetingByStartTs = roomMeetings.copy()
        self.meetingByStartTs.sort(key=lambda k: k.ts_start)

        self.meetingByFinishTs = roomMeetings.copy()
        self.meetingByFinishTs.sort(key=lambda k: k.ts_finish)

        self.pcByConnectTs: list[PeerConnection] = []
        for m in roomMeetings:
            self.pcByConnectTs.extend(m.peerConnections)
        self.pcByLeaveTs: list[PeerConnection] = self.pcByConnectTs.copy()

        self.pcByConnectTs.sort(key=lambda k: k.ts_joined)
        self.pcByLeaveTs.sort(key=lambda k: k.ts_leave)


class RMSRestarter(ABC):
    assignments: RoomMeetingAssignments

    # when downtimes are planned to start (i.e. new version is being rolled out)
    # each new downtime will cancel previous rollout
    startRolloutAt: list[float]
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
    sortedMeetings: RMSSortedMeetings

    nextNodeToRollout: int

    rollouts: list[RMSRollout]

    def __init__(self, meetings: list[RoomMeeting], startRolloutAt: list[float], disruptionBudget: int, nodeRestartsInSec: int,
                 shardsConfig: ShardsConfig, policy: NewNodePolicy):
        self.meetings = meetings
        self.startRolloutAt = startRolloutAt
        self.disruptionBudget = disruptionBudget
        self.nodeRestartsInSec = nodeRestartsInSec
        self.shardsConfig = shardsConfig
        self.newNodePolicy = policy

        self.sortedMeetings = RMSSortedMeetings(self.meetings)
        self.assignments = RoomMeetingAssignments()
        self.finishGraceEvents = []
        self.nodesInGraceIndex = {}
        self.nodesStartupEvents = []
        self.rollouts = []

    def meetingStarted(self, rm: RoomMeeting):
        node = self.newNodePolicy.pickNodeForRoom(rm.ts_start, self.assignments)
        self.assignments.assignRoomMeeting(rm, node, rm.ts_start)
        _logger.debug(f"{formatIsoDate(rm.ts_start)}: meeting started  {rm.id} on node {node}")

    def meetingFinished(self, rm: RoomMeeting):
        currentNode = self.assignments.getCurrentNode(rm, rm.ts_finish)
        self.assignments.releaseRoomMeeting(rm, rm.ts_finish)
        _logger.debug(f"{formatIsoDate(rm.ts_finish)}: meeting finished {rm.id} on node {currentNode}")
        self.tryFinishMaintenanceIfNoMoreMeetings(currentNode, rm.ts_finish)

    def scheduleFinishGrace(self, nodeId: int, graceFinishesTs: float, rollout: RMSRollout):
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
    def tryFinishMaintenanceIfNoMoreMeetings(self, nodeId: int, ts: float):
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

        newFinishGraceEventIndexIndex: int = finishGraceEventIndex
        for i in range(finishGraceEventIndex-1, -1, -1):
            # shift all events that should start after our new maintenance time to +1
            _logger.debug(f"trying index {i}")
            if self.finishGraceEvents[i].ts > ts:
                evt = self.finishGraceEvents[i]
                newIndex = i+1
                _logger.debug(f"shifting event from {i} to {newIndex} because {formatIsoDate(self.finishGraceEvents[i].ts)} is larger than {formatIsoDate(ts)}")
                self.finishGraceEvents[newIndex] = evt
                self.nodesInGraceIndex[evt.nodeId] = newIndex
                newFinishGraceEventIndexIndex = i
            else:
                break

        _logger.debug(f"maintenance of node {nodeId} shifted from {finishGraceEventIndex}:{formatIsoDate(graceFinishEvent.ts)} "
              f"to {newFinishGraceEventIndexIndex}:{formatIsoDate(ts)} because there were no meetings")
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
            if self.finishGraceEvents[newFinishGraceEventIndexIndex+1].ts < self.finishGraceEvents[newFinishGraceEventIndexIndex].ts:
                raise RuntimeError(f"Index: {newFinishGraceEventIndexIndex}. {formatIsoDate(self.finishGraceEvents[newFinishGraceEventIndexIndex+1].ts)} < {formatIsoDate(self.finishGraceEvents[newFinishGraceEventIndexIndex].ts)}")

    def returnNodeToDuty(self, nodeId: int, ts: float):
        _logger.debug(f"{formatIsoDate(ts)}: returning node {nodeId} to duty")

        self.assignments.endNodeMaintenance(nodeId, ts)
        if nodeId in self.nodesInGraceIndex:
            self.nodesInGraceIndex.pop(nodeId)

        self.disruptNodes(ts)

    def scheduleNodeStartup(self, nodeId: int, startupStarts: float):
        _logger.debug(f"{formatIsoDate(startupStarts)}: started startup of node {nodeId}")
        startupFinishes = startupStarts + self.nodeRestartsInSec
        self.nodesStartupEvents.append(
            RMSRestarterEvent(startupFinishes, lambda: self.returnNodeToDuty(nodeId, startupFinishes))
        )

    def nodeGraceStarted(self, nodeId: int, ts: float):
        gracePeriod: float

        if self.assignments.nodeHasMeetings(nodeId, ts):
            # if node has active meetings, it needs to wait for grace period, and then it will restart
            gracePeriod = float(self.newNodePolicy.gracePeriod(ts, self.assignments))
            downtimeFinishes = ts + gracePeriod
            self.scheduleFinishGrace(nodeId, downtimeFinishes, self.rollouts[-1])
            _logger.debug(f"{formatIsoDate(ts)}: started grace period of node {nodeId}")
        else:
            _logger.debug(f"{formatIsoDate(ts)}: grace not needed for node {nodeId}")
            self.scheduleNodeStartup(nodeId, ts)

        self.assignments.startNodeMaintenance(nodeId, ts)

    def disruptNodes(self, ts: float):
        # this happens when rollout of the last node finishes
        if self.nextNodeToRollout == self.shardsConfig.numNodesGlobal() and len(self.assignments.nodesMaintenance) == 0:
            self.rollouts[-1].finish(ts)

        while len(self.assignments.getNodesInMaintenance(ts)) < self.disruptionBudget and \
                self.nextNodeToRollout < self.shardsConfig.numNodesGlobal():
            self.nodeGraceStarted(self.nextNodeToRollout, ts)
            self.nextNodeToRollout += 1

    def nodeGraceFinished(self, nodeId: int, ts: float, rollout: RMSRollout):
        meetingsLeft = self.assignments.getNodeMeetings(nodeId, ts)
        _logger.debug(f"{formatIsoDate(ts)}: finished grace period of node {nodeId}. active meetings: {meetingsLeft}")
        for rm in meetingsLeft:
            newNodeId = self.newNodePolicy.pickNodeForRoom(ts, self.assignments)
            self.newNodePolicy.nodeOfRoomMeetingChanged(ts, nodeId, newNodeId, rm.id)
            self.assignments.assignRoomMeeting(rm, newNodeId, ts)
            rollout.downtime(ts, rm)
            _logger.debug(f"{formatIsoDate(ts)}: reassigning room meeting {rm.id} from node {nodeId} to node {newNodeId}")

        self.scheduleNodeStartup(nodeId, ts)

    def startRollout(self, ts: float):
        # todo start multiple rollouts. register new downtime report for each
        # todo downtime reports should account for all graces started during the downtime. Even if grace finishes during another downtime
        _logger.debug(f"{formatIsoDate(ts)}: starting rollout. ")
        if len(self.rollouts) > 0:
            self.rollouts[-1].finish(ts)
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

    def registerPeerConnectionJoined(self, pc: PeerConnection):
        self.newNodePolicy.peerConnectionAssigned(
            pc.ts_joined,
            self.assignments.getCurrentNodeByRmId(pc.rmId, pc.ts_joined),
            pc.rmId
        )

    def registerPeerConnectionLeft(self, pc: PeerConnection):
        self.newNodePolicy.peerConnectionReleased(
            pc.ts_leave,
            self.assignments.getCurrentNodeByRmId(pc.rmId, pc.ts_leave),
            pc.rmId
        )

    def calculateRestarts(self) -> list[RMSRollout]:
        # order meetings by start date and by end date
        # iterate meeting starts, meeting finishes and node maintenances by ts: assign and unassign meetings
        # (complexity: 2*M*log(M) to sort + 2*M*DISR_BUDGET to merge lists by ts with maintenances )
        #
        # when maintenance starts, close nodes for maintenance
        # when maintenance ends, assign all their unfinished meetings to a new node and calculate downtime by number of participants

        _logger.info(f"Preparing start events: {len(self.sortedMeetings.meetingByStartTs)}")
        meetingStartEvents: list[RMSRestarterEvent] = list(map(
            lambda meeting: RMSRestarterEvent(meeting.ts_start, lambda: self.meetingStarted(meeting)),
            self.sortedMeetings.meetingByStartTs
        ))
        _logger.info(f"Preparing finish events: {len(self.sortedMeetings.meetingByStartTs)}")
        meetingFinishEvents: list[RMSRestarterEvent] = list(map(
            lambda meeting: RMSRestarterEvent(meeting.ts_finish, lambda: self.meetingFinished(meeting)),
            self.sortedMeetings.meetingByFinishTs
        ))

        _logger.info(f"Preparing rollout events: {len(self.startRolloutAt)}")
        startRolloutEvents: list[RMSRestarterEvent] = list(map(
            lambda rolloutTs: RMSRestarterEvent(rolloutTs, lambda: self.startRollout(rolloutTs)),
            self.startRolloutAt
        ))

        _logger.info(f"Preparing pcConnect events: {len(self.sortedMeetings.pcByConnectTs)}")
        pcConnectEvents: list[RMSRestarterEvent] = list(map(
            lambda pc: RMSRestarterEvent(pc.ts_joined, lambda: self.registerPeerConnectionJoined(pc)),
            self.sortedMeetings.pcByConnectTs
        ))

        _logger.info(f"Preparing pcLeave events: {len(self.sortedMeetings.pcByLeaveTs)}")
        pcLeaveEvents: list[RMSRestarterEvent] = list(map(
            lambda pc: RMSRestarterEvent(pc.ts_leave, lambda: self.registerPeerConnectionLeft(pc)),
            self.sortedMeetings.pcByLeaveTs
        ))

        _logger.info(f"Meeting dates range: {formatIsoDate(self.sortedMeetings.meetingByStartTs[0].ts_start)} - {formatIsoDate(self.sortedMeetings.meetingByFinishTs[-1].ts_finish)}")

        restartEventLists: list[list[RMSRestarterEvent]] = [
            meetingStartEvents,
            pcConnectEvents,  # first meetings should start, then they should end,

            # everything else in between connects and disconnects
            startRolloutEvents,
            self.finishGraceEvents,
            self.nodesStartupEvents,

            pcLeaveEvents,  # first pcs need to leave then room can be unassigned
            meetingFinishEvents,
        ]

        traverser = MultiListTimestampTraverser(restartEventLists)
        traverser.traverse()

        return self.rollouts
