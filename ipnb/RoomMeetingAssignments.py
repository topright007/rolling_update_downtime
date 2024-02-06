from abc import ABC
from collections import defaultdict
from datetime import datetime
from rmsutils import *

from RoomMeeting import *


class RoomMeetingAssignments(ABC):
    nodeToRoomMeeting: dict[int, dict[datetime, list[str]]]
    roomMeetingToNode: dict[str, dict[datetime, int]]

    lastNodeDates: dict[int, datetime]
    lastRMDates: dict[str, datetime]

    roomMeetingDict: dict[str, RoomMeeting]

    nodesMaintenance: dict[datetime, set[int]]

    def __init__(self):
        self.nodeToRoomMeeting = defaultdict(lambda: defaultdict(list))
        self.roomMeetingToNode = defaultdict(lambda: defaultdict(int))
        self.lastNodeDates = {}
        self.lastRMDates = {}
        self.roomMeetingDict = {}
        self.nodesMaintenance = {}

    def __str__(self):
        nodeToRoomMeetingResult = defaultdict(lambda: defaultdict(list))
        for node, tsToMeetings in self.nodeToRoomMeeting.items():
            nodeToRoomMeetingResult[node] = {}
            for ts, rmids in tsToMeetings.items():
                nodeToRoomMeetingResult[node][formatIsoDate(ts)] = []
                for rmid in rmids:
                    nodeToRoomMeetingResult[node][formatIsoDate(ts)].append(rmid)
        roomMeetingToNodeResult = defaultdict(lambda: defaultdict(int))
        for rmId, tsToNode in self.roomMeetingToNode.items():
            roomMeetingToNodeResult[rmId] = {}
            for ts, node in tsToNode.items():
                roomMeetingToNodeResult[rmId][formatIsoDate(ts)] = node
        lastNodeDatesResult = {}
        for node, ts in self.lastNodeDates.items():
            lastNodeDatesResult[node] = formatIsoDate(ts)
        lastRMDatesResult = {}
        for rmid, ts in self.lastRMDates.items():
            lastRMDatesResult[rmid] = formatIsoDate(ts)
        return json.dumps({
            "nodeToRoomMeeting": nodeToRoomMeetingResult,
            "roomMeetingToNode": roomMeetingToNodeResult,
            "lastNodeDates": lastNodeDatesResult,
            "lastRMDates": lastRMDatesResult
        }, indent=2)

    def roomMeetingById(self, id: str):
        return self.roomMeetingDict[id]

    def getCurrentNode(self, rm: RoomMeeting, ts: datetime) -> int:
        if rm.id in self.lastRMDates:
            lastRMDate = self.lastRMDates[rm.id]
            assert lastRMDate < ts, f"Room Meeting was last accessed at {formatIsoDate(lastRMDate)}. Can not access it at {ts}"
            return self.roomMeetingToNode[rm.id][lastRMDate]
        return -1

    def assignRoomMeeting(self, rm: RoomMeeting, node: int, ts: datetime):
        curNode: int = self.getCurrentNode(rm, ts)
        if curNode == node:
            return

        self.releaseRoomMeeting(rm, ts)

        self.roomMeetingDict[rm.id] = rm
        rmDates = self.nodeToRoomMeeting[node].keys()
        if node in self.lastNodeDates:
            lastTs = self.lastNodeDates[node]
            assert lastTs <= ts, f"can not assign ts {ts}. TS {lastTs} is already assigned to node {node}"
            newNodeRoomMeetingsBeforeAssignment = self.nodeToRoomMeeting[node][lastTs]
            newNodeRoomMeetingsAfterAssignment = newNodeRoomMeetingsBeforeAssignment.copy()
            newNodeRoomMeetingsAfterAssignment.append(rm.id)
            self.nodeToRoomMeeting[node][ts] = newNodeRoomMeetingsAfterAssignment
        else:
            self.nodeToRoomMeeting[node][ts].append(rm.id)

        self.roomMeetingToNode[rm.id][ts] = node
        self.lastNodeDates[node] = ts
        self.lastRMDates[rm.id] = ts

    def releaseRoomMeeting(self, rm: RoomMeeting, ts: datetime):
        if rm.id in self.lastRMDates:
            lastNodeAssignmentTs = self.lastRMDates[rm.id]
            assert lastNodeAssignmentTs <= ts, f"can not assign ts {ts}. TS {lastNodeAssignmentTs} is already assigned to room {rm.id}"

            prevNodeIdx = self.roomMeetingToNode[rm.id][lastNodeAssignmentTs]

            if prevNodeIdx in self.lastNodeDates:
                prevNodeLastStateTs = self.lastNodeDates[prevNodeIdx]
                assert prevNodeLastStateTs <= ts, f"can not assign ts {ts}. TS {prevNodeLastStateTs} is already assigned to node {prevNodeIdx}"

                prevNodeLastStateRMs = self.nodeToRoomMeeting[prevNodeIdx][prevNodeLastStateTs]

                prevNodeNewStateRMs = prevNodeLastStateRMs.copy()
                try:
                    prevNodeNewStateRMs.remove(rm.id)
                except ValueError:
                    raise Exception(
                        f'Failed to remove {rm.id} by node {prevNodeIdx} ts {formatIsoDate(prevNodeLastStateTs)} and contents {prevNodeNewStateRMs}. It probably have not been assigned to the node')

                # if room meeting has been removed from the room, add record about it
                if prevNodeLastStateRMs != prevNodeNewStateRMs:
                    self.nodeToRoomMeeting[prevNodeIdx][ts] = prevNodeNewStateRMs
                    self.lastNodeDates[prevNodeIdx] = ts
                    self.lastRMDates[rm.id] = ts

    def getNodesInMaintenance(self, ts: datetime) -> set[int]:
        if len(self.nodesMaintenance.keys()) > 0:
            lastMaintenanceTs = list(self.nodesMaintenance)[-1]
            assert lastMaintenanceTs < ts, f"Can not modify maintenance. Last maintenance TS ${lastMaintenanceTs} is newer than ${ts}"
            return self.nodesMaintenance[lastMaintenanceTs].copy()
        return set()

    def startNodeMaintenance(self, node: int, ts: datetime):
        nodesInMaintenance = self.getNodesInMaintenance(ts)
        nodesInMaintenance.add(node)
        self.nodesMaintenance[ts] = nodesInMaintenance

    def endNodeMaintenance(self, node: int, ts: datetime):
        nodesInMaintenance = self.getNodesInMaintenance(ts)
        nodesInMaintenance.discard(node)
        self.nodesMaintenance[ts] = nodesInMaintenance
