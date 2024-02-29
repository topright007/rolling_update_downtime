import argparse

from dt_calc_models import *
import pandas as pd

from RoomMeetingAssignments import *
from PeerConnection import *

from rmsops import *

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

MAX_NUM_ROWS_FOR_DRYRUN = 50000
# MAX_NUM_ROWS_FOR_DRYRUN = 5000000000000
shardsConfig = ShardsConfig([10]*3)

MEETING_ON_SAME_BRIDGE_IDLE_TIMEOUT = 60
ROLLOUT_DT_DURATION = 15
PEER_IDLE_TIMEOUT_SEC = 60
NODE_RESTARTS_IN_SEC = 120

DEFAULT_GRACE_PERIOD_SEC = 60
DEFAULT_DISRUPTION_BUDGET = 3

#to reproduce bugs
seed = random.randrange(0,10000000)
# seed = 3558944
random.seed(seed)
root.info(f"Using random seed {seed}")


parser = argparse.ArgumentParser()
parser.add_argument("-r", "--restart-date")
parser.add_argument("-p", "--policy")
parser.add_argument("--dt-calc-model")
parser.add_argument("-d", "--disruption-budget", type=int)
parser.add_argument("-g", "--grace-period-sec", type=int)
args = parser.parse_args()

restartDateStr = '2023-10-02 13:00:00,000'
if args.restart_date is not None and len(args.restart_date) > 0:
    restartDateStr = args.restart_date
restartDate = parseIsoDate(restartDateStr)
root.info(f"Restart date: {restartDateStr}")

gracePeriodSec = DEFAULT_GRACE_PERIOD_SEC
if args.grace_period_sec is not None:
    gracePeriodSec = args.grace_period_sec
root.info(f"Grace Period sec: {restartDateStr}")

disruptionBudget = DEFAULT_DISRUPTION_BUDGET
if args.disruption_budget is not None:
    disruptionBudget = args.disruption_budget
root.info(f"Disruption budget: {restartDateStr}")

policyStr = 'RandomIslandLeastLoadedNewNodePolicy'
if args.policy is not None and len(args.policy) > 0:
    policyStr = args.policy
policy: NewNodePolicy
match policyStr:
    case 'RandomNewNodePolicy':
        policy = RandomNewNodePolicy(gracePeriodSec, shardsConfig)
    case 'RoundRobinNewNodePolicy':
        policy = RoundRobinNewNodePolicy(gracePeriodSec, shardsConfig)
    case 'LeastLoadedNewNodePolicy':
        policy = LeastLoadedNewNodePolicy(gracePeriodSec, shardsConfig)
    case 'RandomIslandLeastLoadedNewNodePolicy':
        policy = RandomIslandLeastLoadedNewNodePolicy(gracePeriodSec, shardsConfig)
    case _:
        raise f"unknown policy {policyStr}"
root.info(f"Policy: {policyStr}")

dtModelStr = 'IntegratingDTClacModel'
if args.dt_calc_model is not None and len(args.dt_calc_model) > 0:
    dtModelStr = args.dt_calc_model
root.info(f"DT Calc Model: {dtModelStr}")

calls = pd.read_csv('calls_data_week.tsv', header=0, names=['msid','peer_id', 'room_id', 'rsid', 'ts_connected', 'ts_joined', 'ts_leave', 'ts_offer'], delimiter='\t', nrows=MAX_NUM_ROWS_FOR_DRYRUN)
root.info(f"Loaded {len(calls)} events")
roomMeetings: list[RoomMeeting] = loadRoomMeetings(calls, MEETING_ON_SAME_BRIDGE_IDLE_TIMEOUT)

restarter = RMSRestarter(roomMeetings, [restartDate], disruptionBudget, NODE_RESTARTS_IN_SEC, shardsConfig, policy)
restartResult: list[RMSRollout] = restarter.calculateRestarts()

dtmodel: DTCalcModel
match dtModelStr:
    case 'IntegratingDTClacModel':
        dtmodel = IntegratingDTClacModel(restarter.assignments, restartResult, restarter.sortedMeetings, PEER_IDLE_TIMEOUT_SEC, ROLLOUT_DT_DURATION)
    case 'DTOverRolloutPeriod':
        dtmodel = DTOverRolloutPeriod(restarter.assignments, restartResult, restarter.sortedMeetings, PEER_IDLE_TIMEOUT_SEC, ROLLOUT_DT_DURATION)
    case 'DTOverTheWholeWeek':
        dtmodel = DTOverTheWholeWeek(restarter.assignments, restartResult, restarter.sortedMeetings, PEER_IDLE_TIMEOUT_SEC, ROLLOUT_DT_DURATION)
    case _:
        raise f"unknown dt calculation model {dtModelStr}"

chart = dtmodel.totalDowntime()
chart.populateRolloutParameters(
    policyStr=policyStr,
    dtModelStr=dtModelStr,
    gracePeriodSec=gracePeriodSec,
    disruptionBudget=disruptionBudget,
    restartDateStr=restartDateStr
)
root.info(f"Total downtime is {chart.totalDT}")
chart.serialize()
