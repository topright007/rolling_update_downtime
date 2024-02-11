import abc
import logging
import random

from RoomMeetingAssignments import *

_logger = logging.getLogger("policies")

class NewNodePolicy(ABC):
    @abc.abstractmethod
    def pickNodeForRoom(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
        raise 'not implemented'

    def gracePeriod(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
        raise 'not implemented'


class ConstantGracePeriodShardedCluster(NewNodePolicy):
    gracePeriodSec: int
    shardsConfig: ShardsConfig

    def __init__(self, gracePeriodSec: int, shardsConfig: ShardsConfig):
        self.gracePeriodSec = gracePeriodSec
        self.shardsConfig = shardsConfig

    def gracePeriod(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
        return self.gracePeriodSec


# pick a new media server at random
class RandomNewNodePolicy(ConstantGracePeriodShardedCluster):
    def __init__(self, gracePeriodSec: int, shardsConfig: ShardsConfig):
        super().__init__(gracePeriodSec, shardsConfig)

    def pickNodeForRoom(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
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

    def pickNodeForRoom(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
        self.lastSelectedRoundRobinNode = (self.lastSelectedRoundRobinNode + 1) % self.shardsConfig.numNodesGlobal()
        return self.lastSelectedRoundRobinNode


# check out number of sessions on the nodes and pick the least loaded
class LeastLoadedNewNodePolicy(ConstantGracePeriodShardedCluster):

    def __init__(self, gracePeriodSec: int, shardsConfig: ShardsConfig):
        super().__init__(gracePeriodSec, shardsConfig)

    def pickNodeForRoom(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
        return random.randrange(0, 1)


# pick an island at random and pick the least loaded node on it
class RandomIslandLeastLoadedNewNodePolicy(ConstantGracePeriodShardedCluster):
    lastSelectedRoundRobinNode = -1

    def __init__(self, gracePeriodSec: int):
        super().__init__(gracePeriodSec)

    def pickNodeForRoom(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
        return random.randrange(0, 1)
