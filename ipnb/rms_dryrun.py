from RMSRestarter import *
from dt_calc_models import *
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from statistics import mean
import matplotlib.font_manager
# import Pyarrow

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

MAX_NUM_ROWS_FOR_DRYRUN = 500
# MAX_NUM_ROWS_FOR_DRYRUN = 5000000000000

MEETING_ON_SAME_BRIDGE_IDLE_TIMEOUT = 60
ROLLOUT_DT_DURATION = 15
PEER_IDLE_TIMEOUT_SEC = 60
calls = pd.read_csv('calls_data_week.tsv', header=0, names=['msid','peer_id', 'room_id', 'rsid', 'ts_connected', 'ts_joined', 'ts_leave', 'ts_offer'], delimiter='\t', nrows=MAX_NUM_ROWS_FOR_DRYRUN)
roomMeetings: list[RoomMeeting] = loadRoomMeetings(calls, MEETING_ON_SAME_BRIDGE_IDLE_TIMEOUT)

shardsConfig = ShardsConfig([10, 10, 10])
restarter = RMSRestarter(roomMeetings, [parseIsoDate('2023-10-02 13:00:00,000')], 10, 120, shardsConfig, RandomNewNodePolicy(600, shardsConfig))
restartResult: list[RMSRollout] = restarter.calculateRestarts()
chart = IntegratingDTClacModel(restarter.assignments, restartResult, restarter.sortedMeetings, PEER_IDLE_TIMEOUT_SEC).totalDowntime()
root.info(f"Total downtime is {chart.totalDT}")