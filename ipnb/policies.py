import abc
import random

from RoomMeetingAssignments import *


class NewNodePolicy(ABC):
    @abc.abstractmethod
    def pickNodeForRoom(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
        raise 'not implemented'

    def gracePeriod(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
        raise 'not implemented'


class ConstantGracePeriod(NewNodePolicy):
    gracePeriodSec: int

    def __init__(self, gracePeriodSec: int):
        self.gracePeriodSec = gracePeriodSec

    def gracePeriod(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
        return self.gracePeriodSec


# pick a new media server at random
class RandomNewNodePolicy(ConstantGracePeriod):
    numNodes: int

    def __init__(self, gracePeriodSec: int, numNodes: int):
        super().__init__(gracePeriodSec)
        self.numNodes = numNodes

    def pickNodeForRoom(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
        nodesInMaintenance = sorted(list(rmass.getNodesInMaintenance(ts)))
        idxToPick = random.randrange(0, self.numNodes)
        for inMaintenance in nodesInMaintenance:
            if idxToPick <= inMaintenance:
                idxToPick += 1
        return idxToPick


# pick servers sequentially when new conference is needed
class RoundRobinNewNodePolicy(ConstantGracePeriod):
    numNodes: int
    lastSelectedRoundRobinNode: int

    def __init__(self, gracePeriodSec: int, numNodes: int):
        super().__init__(gracePeriodSec)
        self.numNodes = numNodes
        self.lastSelectedRoundRobinNode = -1

    def pickNodeForRoom(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
        return random.randrange(0, 1)


# check out number of sessions on the nodes and pick the least loaded
class LeastLoadedNewNodePolicy(ConstantGracePeriod):

    def __init__(self, gracePeriodSec: int):
        super().__init__(gracePeriodSec)

    def pickNodeForRoom(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
        return random.randrange(0, 1)


# pick an island at random and pick the least loaded node on it
class RandomIslandLeastLoadedNewNodePolicy(ConstantGracePeriod):
    lastSelectedRoundRobinNode = -1

    def __init__(self, gracePeriodSec: int):
        super().__init__(gracePeriodSec)

    def pickNodeForRoom(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
        return random.randrange(0, 1)
