import abc
import logging
import random
from typing import Callable

from sortedcontainers import SortedDict

from RoomMeetingAssignments import *

_logger = logging.getLogger("policies")

class NewNodePolicy(ABC):
    @abc.abstractmethod
    def pickNodeForRoom(self, ts: float, rmass: RoomMeetingAssignments) -> int:
        raise 'not implemented'

    def gracePeriod(self, ts: float, rmass: RoomMeetingAssignments) -> int:
        raise 'not implemented'

    def peerConnectionAssigned(self, ts: float, node: int, rmId: str):
        pass

    def peerConnectionReleased(self, ts: float, node: int, rmId: str):
        pass

    def nodeOfRoomMeetingChanged(self, ts: float, oldNode: int, newNode: int, rmId: str):
        pass


class ConstantGracePeriodShardedCluster(NewNodePolicy):
    gracePeriodSec: int
    shardsConfig: ShardsConfig

    def __init__(self, gracePeriodSec: int, shardsConfig: ShardsConfig):
        self.gracePeriodSec = gracePeriodSec
        self.shardsConfig = shardsConfig

    def gracePeriod(self, ts: float, rmass: RoomMeetingAssignments) -> int:
        return self.gracePeriodSec


# pick a new media server at random
class RandomNewNodePolicy(ConstantGracePeriodShardedCluster):
    def __init__(self, gracePeriodSec: int, shardsConfig: ShardsConfig):
        super().__init__(gracePeriodSec, shardsConfig)

    def pickNodeForRoom(self, ts: float, rmass: RoomMeetingAssignments) -> int:
        nodesInMaintenance = sorted(list(rmass.getNodesInMaintenance(ts)))
        idxToPick = random.randrange(0, self.shardsConfig.numNodesGlobal() - len(nodesInMaintenance))
        for inMaintenance in nodesInMaintenance:
            if idxToPick >= inMaintenance:
                idxToPick += 1
        _logger.debug(f"picked node {idxToPick}. Nodes in maintenance are {nodesInMaintenance}")
        return idxToPick


# pick servers sequentially when new conference is needed
class RoundRobinNewNodePolicy(ConstantGracePeriodShardedCluster):
    numNodes: int
    lastSelectedRoundRobinNode: int

    def __init__(self, gracePeriodSec: int, shardsConfig: ShardsConfig):
        super().__init__(gracePeriodSec, shardsConfig)
        self.lastSelectedRoundRobinNode = -1

    def pickNodeForRoom(self, ts: float, rmass: RoomMeetingAssignments) -> int:
        self.lastSelectedRoundRobinNode = (self.lastSelectedRoundRobinNode + 1) % self.shardsConfig.numNodesGlobal()
        return self.lastSelectedRoundRobinNode


class ShardToPeerConnectionAssignmentsCounter(ABC):
    # sorted one. used to find the least loaded node
    pcNumToNodes: dict[int, set[int]]
    nodeToPcNum: dict[int, int]
    roomToPCNum: dict[str, int]
    lastTs: float

    def __init__(self, nodesInCluster: Iterable[int]):
        self.lastTs = 0.0
        self.pcNumToNodes = SortedDict()
        self.nodeToPCNum = {}
        self.roomToPCNum = defaultdict(lambda: 0)

        allNodes = set()
        for node in nodesInCluster:
            self.nodeToPCNum[node] = 0
            allNodes.add(node)

        self.pcNumToNodes[0] = allNodes

    def assertTS(self, ts: float):
        assert ts >= self.lastTs, f"ts {formatIsoDate(ts)} is less than last ts {formatIsoDate(self.lastTs)}"
        self.lastTs = ts

    # because there's no default dict in sorted dict
    def pcNumToNode(self, pcNum: int) -> set[int]:
        if pcNum not in self.pcNumToNodes:
            result = set()
            self.pcNumToNodes[pcNum] = result
        return self.pcNumToNodes[pcNum]

    # assigns peer connection to the node that rm is assigned to
    def assignPeerConnection(self, ts: float, node: int, rmId: str):
        self.assertTS(ts)

        currentNum = self.nodeToPCNum[node]
        self.nodeToPCNum[node] += 1
        self.roomToPCNum[rmId] += 1
        self.pcNumToNode(currentNum).remove(node)
        self.pcNumToNode(currentNum+1).add(node)

    # releases peer connection from the node that rm is assigned to
    def releasePeerConnection(self, ts: float, node: int, rmId: str):
        self.assertTS(ts)
        currentNum = self.nodeToPCNum[node]
        self.nodeToPCNum[node] -= 1
        self.roomToPCNum[rmId] -= 1
        self.pcNumToNode(currentNum).remove(node)
        self.pcNumToNode(currentNum - 1).add(node)

        if self.roomToPCNum[rmId] == 0:
            del self.roomToPCNum[rmId]

    def reassignRoomMeeting(self, ts: float, oldNode: int, newNode: int, rmId: str):
        self.assertTS(ts)
        numToReassign = self.roomToPCNum[rmId]
        oldNodeOldNum = self.nodeToPCNum[oldNode]
        oldNodeNewNum = oldNodeOldNum - numToReassign
        assert oldNodeNewNum >= 0, f"Can not reassign {numToReassign} pcs from node {oldNode} to node {newNode}. {oldNode} only has {oldNodeOldNum} pcs"
        newNodeOldNum = self.nodeToPCNum[newNode]
        newNodeNewNum = newNodeOldNum + numToReassign
        self.nodeToPCNum[oldNode] = oldNodeNewNum
        self.nodeToPCNum[newNode] = newNodeNewNum
        self.pcNumToNode(oldNodeOldNum).remove(oldNode)
        self.pcNumToNode(oldNodeNewNum).add(oldNode)
        self.pcNumToNode(newNodeOldNum).remove(newNode)
        self.pcNumToNode(newNodeNewNum).add(newNode)

    def getNumPCInRoom(self, rmId: str):
        return self.roomToPCNum[rmId] or 0

    def getLeastLoadedNodes(self, nodeFilter: Callable[[int], bool]) -> list[int]:
        for _, nodes in self.pcNumToNodes.items():
            result = []
            for node in nodes:
                if not nodeFilter(node):
                    continue
                result.append(node)
            if len(result) > 0:
                return result
            # otherwise all least loaded nodes are in maintenance. keep searching
        return []

# check out number of sessions on the nodes and pick the least loaded
class LeastLoadedNewNodePolicy(ConstantGracePeriodShardedCluster):

    globalAssignmentCounter: ShardToPeerConnectionAssignmentsCounter

    def __init__(self, gracePeriodSec: int, shardsConfig: ShardsConfig):
        super().__init__(gracePeriodSec, shardsConfig)
        allNodesRange = range(0, self.shardsConfig.numNodesGlobal())
        self.globalAssignmentCounter = ShardToPeerConnectionAssignmentsCounter(allNodesRange)

    def pickNodeFromListOfNodes(self, ts: float, rmass: RoomMeetingAssignments, leastLoadedFinder: ShardToPeerConnectionAssignmentsCounter) -> int:
        leastLoadedNodes = leastLoadedFinder.getLeastLoadedNodes(lambda theNode: not rmass.isNodeInMaintenance(theNode, ts))

        assert len(leastLoadedNodes) > 0, "Failed to find any node to pick. Must be a bug"
        randomLeastLoadedIndex = random.randrange(0, len(leastLoadedNodes))
        return leastLoadedNodes[randomLeastLoadedIndex]

    def pickNodeForRoom(self, ts: float, rmass: RoomMeetingAssignments) -> int:
        return self.pickNodeFromListOfNodes(ts, rmass, self.globalAssignmentCounter)

    def peerConnectionAssigned(self, ts: float, node: int, rmId: str):
        self.globalAssignmentCounter.assignPeerConnection(ts, node, rmId)

    def peerConnectionReleased(self, ts: float, node: int, rmId: str):
        self.globalAssignmentCounter.releasePeerConnection(ts, node, rmId)

    def nodeOfRoomMeetingChanged(self, ts: float, oldNode: int, newNode: int, rmId: str):
        self.globalAssignmentCounter.reassignRoomMeeting(ts, oldNode, newNode, rmId)


# pick an island at random and pick the least loaded node on it
class RandomIslandLeastLoadedNewNodePolicy(LeastLoadedNewNodePolicy):
    leastLoadedCounters: dict[int, ShardToPeerConnectionAssignmentsCounter]

    def __init__(self, gracePeriodSec: int, shardsConfig: ShardsConfig):
        super().__init__(gracePeriodSec, shardsConfig)
        self.leastLoadedCounters = {}
        for numCluster in range(0, len(self.shardsConfig.shards)):
            self.leastLoadedCounters[numCluster] = ShardToPeerConnectionAssignmentsCounter(
                self.shardsConfig.nodesInCluster(numCluster)
            )

    def pickNodeForRoom(self, ts: float, rmass: RoomMeetingAssignments) -> int:
        clusterToPick = random.randrange(0, self.shardsConfig.numClusters())
        return self.pickNodeFromListOfNodes(ts, rmass, self.leastLoadedCounters[clusterToPick])
