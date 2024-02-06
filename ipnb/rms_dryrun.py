from RMSRestarter import *
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from statistics import mean
import matplotlib.font_manager
# import Pyarrow

from RoomMeetingAssignments import *
from PeerConnection import *

from rmsops import *

MAX_NUM_ROWS_FOR_DRYRUN = 500
# MAX_NUM_ROWS_FOR_DRYRUN = 5000000000000
calls = pd.read_csv('calls_data_week.tsv', header=0, names=['msid','peer_id', 'room_id', 'rsid', 'ts_connected', 'ts_joined', 'ts_leave', 'ts_offer'], delimiter='\t', nrows=MAX_NUM_ROWS_FOR_DRYRUN)
roomMeetings: list[RoomMeeting] = loadRoomMeetings(calls, 60)

restarter = RMSRestarter(roomMeetings, [parseIsoDate('2023-10-04 16:00:00,000')], 10, RandomNewNodePolicy(60, 100))
restartResult: list[RMSRestartResult] = restarter.measureDT()
for dt in restartResult:
    print(f"downtime {formatIsoDate(dt.downtimeStart)} - {formatIsoDate(dt.downtimeFinish)}. Total DT: {dt.totalDTSec} seconds")