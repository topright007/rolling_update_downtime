import logging
from abc import ABC
from collections import defaultdict
from typing import Iterable

from rmsutils import *

from RoomMeeting import *

_logger = logging.getLogger("RoomMeetingAssignments")


class ShardsConfig(ABC):
    shards: list[int]

    def __init__(self, shards: list[int]):
        self.shards = shards

    def pickGlobal(self, globalIndex: int, globalIndexesInMaintenance: set[int]) -> int:
        pass

    def pickInCluster(self, clusterNumber: int, indexInCluster: int, globalIndexesInMaintenance: set[int]) -> int:
        pass

    def globalToCluster(self, globalNodeIndex: int) -> tuple[int, int]:
        pass

    def clusterToGlobal(self, clusterNumber: int, indexInCluster: int) -> int:
        pass

    def numNodesGlobal(self) -> int:
        return sum(self.shards)

    def numClusters(self) -> int:
        return len(self.shards)

    def nodesInCluster(self, cluster: int) -> Iterable[int]:
        firstNode = 0
        for c in range(0, cluster):
            firstNode += self.shards[c]
        lastNode  = firstNode + self.shards[cluster]
        return range(firstNode, lastNode)


class RoomMeetingAssignments(ABC):
    nodeToRoomMeeting: dict[int, dict[float, list[RoomMeeting]]]
    roomMeetingToNode: dict[str, dict[float, int]]

    lastNodeDates: dict[int, float]
    lastRMDates: dict[str, float]

    roomMeetingDict: dict[str, RoomMeeting]

    nodesMaintenance: dict[float, set[int]]

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
            for ts, rms in tsToMeetings.items():
                nodeToRoomMeetingResult[node][formatIsoDate(ts)] = []
                for rm in rms:
                    nodeToRoomMeetingResult[node][formatIsoDate(ts)].append(rm.id)
        roomMeetingToNodeResult = defaultdict(lambda: defaultdict(int))
        for rmId, tsToNode in self.roomMeetingToNode.items():
            for ts, node in tsToNode.items():
                roomMeetingToNodeResult[rmId][formatIsoDate(ts)] = node
        lastNodeDatesResult = {}
        for node, ts in self.lastNodeDates.items():
            lastNodeDatesResult[node] = formatIsoDate(ts)
        lastRMDatesResult = {}
        for rm, ts in self.lastRMDates.items():
            lastRMDatesResult[rm] = formatIsoDate(ts)
        return json.dumps({
            "nodeToRoomMeeting": nodeToRoomMeetingResult,
            "roomMeetingToNode": roomMeetingToNodeResult,
            "lastNodeDates": lastNodeDatesResult,
            "lastRMDates": lastRMDatesResult
        }, indent=2)

    def roomMeetingById(self, id: str):
        return self.roomMeetingDict[id]

    def getCurrentNode(self, rm: RoomMeeting, ts: float) -> int:
        if rm.id in self.lastRMDates:
            lastRMDate = self.lastRMDates[rm.id]
            assert lastRMDate <= ts, f"Room Meeting was last accessed at {formatIsoDate(lastRMDate)}. Can not access it at {ts}"
            return self.roomMeetingToNode[rm.id][lastRMDate]
        return -1

    def nodeHasMeetings(self, nodeId: int, ts: float) -> bool:
        return len(self.getNodeMeetings(nodeId, ts)) > 0

    def getNodeMeetings(self, nodeId: int, ts: float) -> list[RoomMeeting]:
        mappings = self.nodeToRoomMeeting[nodeId]
        if len(mappings) == 0:
            return []
        lastMappedTs = list(mappings)[-1]
        assert ts >= lastMappedTs, f"requested timestamp {formatIsoDate(ts)} is before the last mapped ts {formatIsoDate(lastMappedTs)}"
        _logger.debug(f"mapping: {formatIsoDate(lastMappedTs)}: meetings on node {nodeId}: {mappings[lastMappedTs]}")
        return mappings[lastMappedTs]

    def assignRoomMeeting(self, rm: RoomMeeting, node: int, ts: float):
        curNode: int = self.getCurrentNode(rm, ts)
        if curNode == node:
            return

        self.releaseRoomMeeting(rm, ts)

        self.roomMeetingDict[rm.id] = rm
        if node in self.lastNodeDates:
            lastTs = self.lastNodeDates[node]
            assert lastTs <= ts, f"can not assign ts {ts}. TS {lastTs} is already assigned to node {node}"
            newNodeRoomMeetingsBeforeAssignment = self.nodeToRoomMeeting[node][lastTs]
            newNodeRoomMeetingsAfterAssignment = newNodeRoomMeetingsBeforeAssignment.copy()
            newNodeRoomMeetingsAfterAssignment.append(rm)
            self.nodeToRoomMeeting[node][ts] = newNodeRoomMeetingsAfterAssignment
        else:
            self.nodeToRoomMeeting[node][ts].append(rm)

        self.roomMeetingToNode[rm.id][ts] = node
        self.lastNodeDates[node] = ts
        self.lastRMDates[rm.id] = ts

    def releaseRoomMeeting(self, rm: RoomMeeting, ts: float):
        if rm.id in self.lastRMDates:
            lastNodeAssignmentTs = self.lastRMDates[rm.id]
            assert lastNodeAssignmentTs <= ts, f"can not assign ts {ts}. TS {lastNodeAssignmentTs} is already assigned to room {rm.id}"

            prevNodeIdx = self.roomMeetingToNode[rm.id][lastNodeAssignmentTs]

            if prevNodeIdx in self.lastNodeDates:
                prevNodeLastStateTs = self.lastNodeDates[prevNodeIdx]
                assert prevNodeLastStateTs <= ts, f"can not assign ts {ts}. TS {prevNodeLastStateTs} is already assigned to node {prevNodeIdx}"

                prevNodeLastStateRMs = self.nodeToRoomMeeting[prevNodeIdx][prevNodeLastStateTs]

                prevNodeNewStateRMs: list[RoomMeeting] = prevNodeLastStateRMs.copy()
                try:
                    prevNodeNewStateRMs = list(filter(lambda x: x.id != rm.id, prevNodeNewStateRMs))
                except ValueError:
                    raise Exception(
                        f'Failed to remove {rm.id} by node {prevNodeIdx} ts {formatIsoDate(prevNodeLastStateTs)} and contents {prevNodeNewStateRMs}. It probably have not been assigned to the node')

                # if room meeting has been removed from the room, add record about it
                if prevNodeLastStateRMs != prevNodeNewStateRMs:
                    self.nodeToRoomMeeting[prevNodeIdx][ts] = prevNodeNewStateRMs
                    self.lastNodeDates[prevNodeIdx] = ts
                    self.lastRMDates[rm.id] = ts

    def getNodesInMaintenance(self, ts: float) -> set[int]:
        if len(self.nodesMaintenance.keys()) > 0:
            lastMaintenanceTs = list(self.nodesMaintenance)[-1]
            assert lastMaintenanceTs <= ts, f"Can not modify maintenance. Last maintenance TS ${lastMaintenanceTs} is newer than ${ts}"
            return self.nodesMaintenance[lastMaintenanceTs].copy()
        return set()

    def isNodeInMaintenance(self, nodeId: int, ts: float):
        return nodeId in self.getNodesInMaintenance(ts)

    def startNodeMaintenance(self, node: int, ts: float):
        nodesInMaintenance = self.getNodesInMaintenance(ts)
        nodesInMaintenance.add(node)
        self.nodesMaintenance[ts] = nodesInMaintenance

    def endNodeMaintenance(self, node: int, ts: float):
        nodesInMaintenance = self.getNodesInMaintenance(ts)
        nodesInMaintenance.discard(node)
        self.nodesMaintenance[ts] = nodesInMaintenance

    def getActiveNodesToPeerConnectionsNum(self, ts: float, nodes: Iterable[int]) -> dict[int, int]:
        result = {}
        for node in nodes:
            if self.isNodeInMaintenance(node, ts):
                continue
            tsMapping = self.nodeToRoomMeeting[node]
            if node not in self.lastNodeDates:
                result[node] = 0
                continue

            nodeTs = self.lastNodeDates[node]
            assert nodeTs <= ts, f"trying to operate on node last accessed at {formatIsoDate(nodeTs)} with an earlier date {formatIsoDate(ts)}"
            cnt = 0
            for rm in tsMapping[nodeTs]:
                for pc in rm.peerConnections:
                    if pc.ts_joined <= ts <= pc.ts_leave:
                        cnt += 1
            result[node] = cnt
        return result

