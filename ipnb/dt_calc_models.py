import datetime
from datetime import timedelta
from abc import ABC, abstractmethod

import jsons
import pandas as pd

from RoomMeetingAssignments import *
from RMSRestarter import *

_logger = logging.getLogger("dt_calc_models")


@dataclass
class RMSDowntimeDT(ABC):
    ts: float
    dt: float
    rmTotal: int
    rmInterrupted: int
    pcTotal: int
    pcInterrupted: int

    def __init__(self):
        self.dt = 0
        self.pcTotal = 0
        self.rmTotal = 0
        self.rmInterrupted = 0
        self.pcInterrupted = 0


@dataclass
class RMSDowntimeChart(ABC):
    dates: list[float]
    datetimes: list[datetime]
    rmInterrupted: list[int]
    pcInterrupted: list[int]
    dts: list[float]
    totalDT: float
    rolloutBoundaries: list[tuple[float, float]]

    policyStr: str
    dtModelStr: str
    gracePeriodSec: int
    disruptionBudget: int
    restartDateStr: str

    def __init__(self,
                 unorderedData: dict[float, RMSDowntimeDT] = None,
                 restartResult: list[RMSRollout] = None
                 ):

        self.dates = []
        self.datetimes = []
        self.dts = []
        self.pcTotal = []
        self.rmTotal = []
        self.rmInterrupted = []
        self.pcInterrupted = []
        self.rolloutBoundaries = []
        self.totalDT = 0

        if unorderedData is None:
            return

        for date, dt in sorted(unorderedData.items()):
            self.dates.append(date)
            self.datetimes.append(datetime.fromtimestamp(date))
            self.dts.append(dt.dt)
            self.pcTotal.append(dt.pcTotal)
            self.rmTotal.append(dt.rmTotal)
            self.rmInterrupted.append(dt.rmInterrupted)
            self.pcInterrupted.append(dt.pcInterrupted)
            self.totalDT += dt.dt
        for rollout in restartResult:
            self.rolloutBoundaries.append((rollout.startTs, rollout.finishTs))


    def populateRolloutParameters(self,
                                  policyStr: str,
                                  dtModelStr: str,
                                  gracePeriodSec: int,
                                  disruptionBudget: int,
                                  restartDateStr: str):
        self.policyStr = policyStr
        self.dtModelStr = dtModelStr
        self.gracePeriodSec = gracePeriodSec
        self.disruptionBudget = disruptionBudget
        self.restartDateStr = restartDateStr

    def serialize(self, fname: str = None):
        if fname is None:
            fname = f"result_{self.policyStr}.dtmodel_{self.dtModelStr}.grace_{self.gracePeriodSec}.disr_{self.disruptionBudget}.at_{self.restartDateStr[0:19].replace(' ' , 'T')}.json"
        jsonStr = jsons.dumps(self)
        with open(fname, "w") as text_file:
            text_file.write(jsonStr)

    def parse(fname: str):
        with open(fname, "r") as text_file:
            jsonstr = text_file.read()
            return jsons.loads(jsonstr, cls=RMSDowntimeChart)


class DTCalcModel(ABC):
    assignments: RoomMeetingAssignments
    rollouts: list[RMSRollout]
    sortedMeetings: RMSSortedMeetings
    reconnectDowntimeSec: int

    def __init__(self, assignments: RoomMeetingAssignments, rollouts: list[RMSRollout],
                 sortedMeetings: RMSSortedMeetings, reconnectDowntimeSec: int):
        self.assignments = assignments
        self.rollouts = rollouts
        self.sortedMeetings = sortedMeetings
        self.reconnectDowntimeSec = reconnectDowntimeSec

    @abstractmethod
    def totalDowntime(self) -> RMSDowntimeChart:
        pass


class TotalRMandPCGraphingModel(DTCalcModel):
    peerIdleTimeoutSec: int
    pcBuckets: dict[float, list[PeerConnection]]
    rmBuckets: dict[float, list[RoomMeeting]]

    def __init__(self,
                 assignments: RoomMeetingAssignments,
                 rollouts: list[RMSRollout],
                 sortedMeetings: RMSSortedMeetings,
                 peerIdleTimeoutSec: int,
                 reconnectDowntimeSec: int):
        super().__init__(assignments, rollouts, sortedMeetings, reconnectDowntimeSec)
        self.peerIdleTimeoutSec = peerIdleTimeoutSec

        self.pcBuckets = defaultdict(list)
        self.rmBuckets = defaultdict(list)
        self.indexPCsAndRMs()

    def floorTime(self, toFloor: float, freqSec: int) -> float:
        epochSeconds = int(toFloor)
        epochSeconds = int(epochSeconds / freqSec) * freqSec
        return float(epochSeconds)

    def addToFlooredBuckets(self, beginTs: float, endTs: float, buckets: dict[float, any], toAdd: any):
        rm_start_floor = self.floorTime(beginTs, self.peerIdleTimeoutSec)
        rm_finish_floor = self.floorTime(endTs, self.peerIdleTimeoutSec)
        ts = rm_start_floor
        leave_timestamp_sec = rm_finish_floor
        while ts < leave_timestamp_sec:
            buckets[ts].append(toAdd)
            ts += self.peerIdleTimeoutSec

    def indexPCsAndRMs(self):
        _logger.info("Starting calculation of total downtime via integrating model")
        for rm in self.sortedMeetings.meetingByStartTs:
            self.addToFlooredBuckets(rm.ts_start, rm.ts_finish, self.rmBuckets, rm)
            for pc in rm.peerConnections:
                self.addToFlooredBuckets(pc.ts_joined, pc.ts_leave, self.pcBuckets, pc)
        _logger.info("Finished indexing of active users by time buckets")

    def addDTDelta(self, dtIncrements: dict[float, RMSDowntimeDT], ts: float, deltaDT: float, rmInterrupted: int, pcInterrupted: int):
        ts_floor = self.floorTime(ts, self.peerIdleTimeoutSec)
        num_pc = len(self.pcBuckets[ts_floor])
        num_rm = len(self.rmBuckets[ts_floor])

        theDt = dtIncrements[ts_floor]
        theDt.ts = ts_floor
        theDt.pcTotal = num_pc
        theDt.rmTotal = num_rm
        theDt.dt += deltaDT
        theDt.rmInterrupted += rmInterrupted
        theDt.pcInterrupted += pcInterrupted


# model that takes the users suffering from DT and adds downtime corresponding to the number of users currently present
class IntegratingDTClacModel(TotalRMandPCGraphingModel):
    peerIdleTimeoutSec: int

    def __init__(self,
                 assignments: RoomMeetingAssignments,
                 rollouts: list[RMSRollout],
                 sortedMeetings: RMSSortedMeetings,
                 peerIdleTimeoutSec: int,
                 reconnectDowntimeSec: int):
        super().__init__(assignments, rollouts, sortedMeetings, peerIdleTimeoutSec, reconnectDowntimeSec)

    def totalDowntime(self) -> RMSDowntimeChart:
        dtIncrements: dict[float, RMSDowntimeDT] = defaultdict(lambda: RMSDowntimeDT())
        for rollout in self.rollouts:
            for dt in rollout.downtimes:
                ts_floor = self.floorTime(dt.ts, self.peerIdleTimeoutSec)
                num_pc = len(self.pcBuckets[ts_floor])
                dtDelta = float(len(dt.rm.peerConnections)) * float(self.reconnectDowntimeSec) / float(num_pc)

                self.addDTDelta(
                    dtIncrements,
                    dt.ts,
                    dtDelta,
                    1,
                    len(dt.rm.peerConnections)
                )

        _logger.info("Finished calculation of total downtime in integrating model")
        return RMSDowntimeChart(unorderedData=dtIncrements, restartResult=self.rollouts)


class DTOverTimePeriod(TotalRMandPCGraphingModel):
    def __init__(self,
                 assignments: RoomMeetingAssignments,
                 rollouts: list[RMSRollout],
                 sortedMeetings: RMSSortedMeetings,
                 peerIdleTimeoutSec: int,
                 reconnectDowntimeSec: int):
        super().__init__(assignments, rollouts, sortedMeetings, peerIdleTimeoutSec, reconnectDowntimeSec)

    def totalDowntimeOfRolloutForPeriod(self, allDtIncrements: dict[float, RMSDowntimeDT], rollout: RMSRollout, periodStart: float, periodEnd: float):
        totalUserSeconds = 0
        dtIncrements: dict[float, RMSDowntimeDT] = defaultdict(lambda: RMSDowntimeDT())
        for ts_floor, peerConnections in self.pcBuckets.items():
            if periodStart <= ts_floor <= periodEnd:
                totalUserSeconds += len(peerConnections) * self.peerIdleTimeoutSec

        for dt in rollout.downtimes:
            deltaDT = float(len(dt.rm.peerConnections)*self.reconnectDowntimeSec)
            ts_floor = self.floorTime(dt.ts, self.peerIdleTimeoutSec)
            num_pc = len(self.pcBuckets[ts_floor])
            self.addDTDelta(dtIncrements, dt.ts, deltaDT, 1, num_pc)

        # normalize by the total number of user*seconds there were in the rollout
        for ts in dtIncrements.keys():
            dtIncrements[ts].dt = dtIncrements[ts].dt / float(totalUserSeconds)

        allDtIncrements.update(dtIncrements)


class DTOverRolloutPeriod(DTOverTimePeriod):
    def __init__(self,
                 assignments: RoomMeetingAssignments,
                 rollouts: list[RMSRollout],
                 sortedMeetings: RMSSortedMeetings,
                 peerIdleTimeoutSec: int,
                 reconnectDowntimeSec: int):
        super().__init__(assignments, rollouts, sortedMeetings, peerIdleTimeoutSec, reconnectDowntimeSec)

    def totalDowntime(self) -> RMSDowntimeChart:
        allDtIncrements: dict[float, RMSDowntimeDT] = defaultdict(lambda: RMSDowntimeDT())
        for rollout in self.rollouts:
            rollout_start_ts_floor = self.floorTime(rollout.startTs, self.peerIdleTimeoutSec)
            rollout_finish_ts_floor = self.floorTime(rollout.finishTs + self.peerIdleTimeoutSec, self.peerIdleTimeoutSec)

            self.totalDowntimeOfRolloutForPeriod(allDtIncrements, rollout, rollout_start_ts_floor, rollout_finish_ts_floor)

        _logger.info("Finished calculation of total downtime via normalization over rollout period")
        return RMSDowntimeChart(unorderedData=allDtIncrements, restartResult=self.rollouts)


class DTOverTheWholeWeek(DTOverTimePeriod):
    def __init__(self,
                 assignments: RoomMeetingAssignments,
                 rollouts: list[RMSRollout],
                 sortedMeetings: RMSSortedMeetings,
                 peerIdleTimeoutSec: int,
                 reconnectDowntimeSec: int):
        super().__init__(assignments, rollouts, sortedMeetings, peerIdleTimeoutSec, reconnectDowntimeSec)

    def totalDowntime(self) -> RMSDowntimeChart:
        allDtIncrements: dict[float, RMSDowntimeDT] = defaultdict(lambda: RMSDowntimeDT())
        for rollout in self.rollouts:
            rolloutStartDate = datetime.fromtimestamp(rollout.startTs).date()
            rolloutStartWeekDate = rolloutStartDate - timedelta(days=rolloutStartDate.weekday())
            rolloutEndWeekDate = rolloutStartDate + timedelta(days=7)
            rolloutStartWeekDateTime = datetime.fromisoformat(rolloutStartWeekDate.isoformat())
            rolloutEndWeekDateTime = datetime.fromisoformat(rolloutEndWeekDate.isoformat())
            week_start_ts_floor = self.floorTime(rolloutStartWeekDateTime.timestamp(), self.peerIdleTimeoutSec)
            week_finish_ts_floor = self.floorTime(rolloutEndWeekDateTime.timestamp() + self.peerIdleTimeoutSec, self.peerIdleTimeoutSec)

            self.totalDowntimeOfRolloutForPeriod(allDtIncrements, rollout, week_start_ts_floor, week_finish_ts_floor)

        _logger.info("Finished calculation of total downtime via normalization over rollout period")
        return RMSDowntimeChart(unorderedData=allDtIncrements, restartResult=self.rollouts)


class TotalDTCalcModel(DTOverTimePeriod):
    def __init__(self,
                 assignments: RoomMeetingAssignments,
                 rollouts: list[RMSRollout],
                 sortedMeetings: RMSSortedMeetings,
                 peerIdleTimeoutSec: int,
                 reconnectDowntimeSec: int):
        super().__init__(assignments, rollouts, sortedMeetings, peerIdleTimeoutSec, reconnectDowntimeSec)

    def totalDowntime(self) -> RMSDowntimeChart:
        allDtIncrements: dict[float, RMSDowntimeDT] = defaultdict(lambda: RMSDowntimeDT())
        for rollout in self.rollouts:
            for dt in rollout.downtimes:
                deltaDT = float(len(dt.rm.peerConnections) * self.reconnectDowntimeSec)
                ts_floor = self.floorTime(dt.ts, self.peerIdleTimeoutSec)
                num_pc = len(self.pcBuckets[ts_floor])
                self.addDTDelta(allDtIncrements, dt.ts, deltaDT, 1, num_pc)

        _logger.info("Finished calculation of total downtime via TotalDTCalcModel")
        return RMSDowntimeChart(unorderedData=allDtIncrements, restartResult=self.rollouts)