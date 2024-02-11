from abc import ABC, abstractmethod

import pandas as pd

from RoomMeetingAssignments import *
from RMSRestarter import *

_logger = logging.getLogger("dt_calc_models")


@dataclass
class RMSDowntimeDT(ABC):
    ts: datetime
    dt: float
    rmInterrupted: int
    pcInterrupted: int

    def __init__(self):
        self.dt = 0
        self.rmInterrupted = 0
        self.pcInterrupted = 0


@dataclass
class RMSDowntimeChart(ABC):
    dates: list[datetime]
    rmInterrupted: list[int]
    pcInterrupted: list[int]
    dts: list[float]
    totalDT: float

    def __init__(self, unorderedData: dict[datetime, RMSDowntimeDT] = None, sourceFname: str = None):
        self.dates = []
        self.dts = []
        self.rmInterrupted = []
        self.pcInterrupted = []
        self.totalDT = 0
        if unorderedData is not None:
            for date, dt in sorted(unorderedData.items()):
                self.dates.append(date)
                self.dts.append(dt.dt)
                self.rmInterrupted.append(dt.rmInterrupted)
                self.pcInterrupted.append(dt.pcInterrupted)
                self.totalDT += dt.dt
        elif sourceFname is not None:
            self.parse(sourceFname)
        else:
            raise "unorderedData or sourceFname are required"

    def serialize(self, fname: str):
        df = pd.DataFrame(data={'dates': self.dates, 'dts': self.dts})
        df.to_csv(fname, index=False, sep='\t')

    def parse(self, fname: str):
        df = pd.read_csv(fname, header=0,
                    names=['dates', 'dts'],
                    delimiter='\t')
        self.dates = list(map(lambda dat: parseIsoDate(dat), df['dates']))
        self.dts = df['dts']
        self.totalDT = sum(self.dts)


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

    def floorTime(self, toFloor: datetime, freqSec: int):
        epochSeconds = int(toFloor.timestamp())
        epochSeconds = int(epochSeconds / freqSec) * freqSec
        return datetime.fromtimestamp(epochSeconds)


    def addToFlooredBuckets(self, begin: datetime, end: datetime, buckets: dict[datetime, any], toAdd: any):
        rm_start_floor = self.floorTime(begin, self.peerIdleTimeoutSec)
        rm_finish_floor = self.floorTime(end, self.peerIdleTimeoutSec)
        ts = rm_start_floor.timestamp()
        leave_timestamp_sec = rm_finish_floor.timestamp()
        while ts < leave_timestamp_sec:
            buckets[datetime.fromtimestamp(ts)].append(toAdd)
            ts += self.peerIdleTimeoutSec


    def totalDowntime(self) -> RMSDowntimeChart:
        _logger.info("Starting calculation of total downtime via integrating model")
        freq = f'{self.peerIdleTimeoutSec}s'
        pcBuckets: dict[datetime, list[PeerConnection]] = defaultdict(list)
        rmBuckets: dict[datetime, list[RoomMeeting]] = defaultdict(list)

        # index
        for rm in self.sortedMeetings.meetingByStartTs:
            self.addToFlooredBuckets(rm.ts_start, rm.ts_finish, rmBuckets, rm)
            for pc in rm.peerConnections:
                self.addToFlooredBuckets(pc.ts_joined, pc.ts_leave, pcBuckets, pc)
        _logger.info("Finished indexing of active users by time buckets")

        dtIncrements: dict[datetime, RMSDowntimeDT] = defaultdict(lambda: RMSDowntimeDT())
        for rollout in self.rollouts:
            for dt in rollout.downtimes:
                dt_floor = pd.Timestamp(dt.ts).floor(freq)
                num_pc = len(pcBuckets[dt_floor])
                num_rm = len(rmBuckets[dt_floor])
                dtDelta = float(len(dt.rm.peerConnections)) / float(num_pc)

                theDt = dtIncrements[dt_floor]
                theDt.ts = dt_floor
                theDt.dt += dtDelta
                theDt.rmInterrupted += num_rm
                theDt.pcInterrupted += num_pc

        _logger.info("Finished calculation of total downtime in integrating model")
        return RMSDowntimeChart(unorderedData=dtIncrements)
