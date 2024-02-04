import abc
import random

from RoomMeetingAssignments import *


class NewNodePolicy(ABC):
    @abc.abstractmethod
    def pickNodeForRoom(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
        raise 'not implemented'


# pick a new media server at random
class RandomNewNodePolicy(NewNodePolicy):
    numNodes: int

    def __init__(self, numNodes: int):
        self.numNodes = numNodes

    def pickNodeForRoom(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
        return random.randrange(0, self.numNodes)


# pick servers sequentially when new conference is needed
class RoundRobinNewNodePolicy(NewNodePolicy):
    lastSelectedRoundRobinNode = -1

    def pickNodeForRoom(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
        return random.randrange(0, 1)


# check out number of sessions on the nodes and pick the least loaded
class LeastLoadedNewNodePolicy(NewNodePolicy):

    def pickNodeForRoom(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
        return random.randrange(0, 1)


# pick an island at random and pick the least loaded node on it
class RandomIslandLeastLoadedNewNodePolicy(NewNodePolicy):
    lastSelectedRoundRobinNode = -1

    def pickNodeForRoom(self, ts: datetime, rmass: RoomMeetingAssignments) -> int:
        return random.randrange(0, 1)
