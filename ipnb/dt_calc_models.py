from abc import ABC, abstractmethod

import pandas as pd

from RoomMeetingAssignments import *
from RMSRestarter import *


@dataclass
class RMSDowntimeChart(ABC):
    dates: list[datetime]
    dts: list[float]
    totalDT: float

    def __init__(self, unordered: dict[datetime, float]):
        self.dates = []
        self.dts = []
        self.totalDT = 0
        for date, dt in sorted(unordered.items()):
            self.dates.append(date)
            self.dts.append(dt)
            self.totalDT += dt


class DTCalcModel(ABC):
    assignments: RoomMeetingAssignments
    rollouts: list[RMSRollout]
    sortedMeetings: RMSSortedMeetings

    def __init__(self, assignments: RoomMeetingAssignments, rollouts: list[RMSRollout],
                 sortedMeetings: RMSSortedMeetings):
        self.assignments = assignments
        self.rollouts = rollouts
        self.sortedMeetings = sortedMeetings

    @abstractmethod
    def totalDowntime(self) -> RMSDowntimeChart:
        pass


# model that takes the users suffering from DT and adds downtime corresponding to the number of users currently present
class IntegratingDTClacModel(DTCalcModel):
    peerIdleTimeoutSec: int

    def __init__(self,
                 assignments: RoomMeetingAssignments,
                 rollouts: list[RMSRollout],
                 sortedMeetings: RMSSortedMeetings,
                 peerIdleTimeoutSec: int):
        super().__init__(assignments, rollouts, sortedMeetings)
        self.peerIdleTimeoutSec = peerIdleTimeoutSec

    def totalDowntime(self) -> RMSDowntimeChart:
        freq = f'{self.peerIdleTimeoutSec}s'
        pcBuckets: dict[datetime, list[PeerConnection]] = defaultdict(list)
        # index
        for rm in self.sortedMeetings.meetingByStartTs:
            for pc in rm.peerConnections:
                join_floor = pd.Timestamp(pc.ts_joined).floor(freq)
                leave_floor = pd.Timestamp(pc.ts_leave).floor(freq)
                for ts in list(pd.date_range(join_floor, leave_floor, freq=freq)):
                    pcBuckets[ts].append(pc)

        dtIncrements: dict[datetime, float] = defaultdict(lambda: 0)
        for rollout in self.rollouts:
            for dt in rollout.downtimes:
                dt_floor = pd.Timestamp(dt.ts).floor(freq)
                num_pc = len(pcBuckets[dt_floor])
                dtDelta = float(len(dt.rm.peerConnections)) / float(num_pc)
                dtIncrements[dt_floor] += dtDelta

        return RMSDowntimeChart(dtIncrements)
