from rmsops import *
from RoomMeetingAssignments import *
from RMSRestarter import *

def run_tests(meetingOnTheSameBridgeIdleTimeout) :
    # RMSConnection tests
    conn = RMSConnection(
        'room_id',
        'peer_id',
        'msid',
        'rsid',
        '2023-10-04 12:53:13,550',
        '2023-10-04 12:54:13,550',
        '2023-10-04 12:55:13,550'
    )
    assert conn.room_id == 'room_id'
    assert conn.peer_id == 'peer_id'
    assert conn.msid == 'msid'
    assert conn.rsid == 'rsid'

    try:
        conn = RMSConnection('fdsa', 'fdsa', 'fdsa', 'fdsa', float('nan'), '2023-10-04 12:54:13,550',
                             '2023-10-04 12:55:13,550')
    except RMSConnectionCreationException as e:
        assert e.reason == 'joined'

    try:
        conn = RMSConnection('fdsa', 'fdsa', 'fdsa', 'fdsa', '2023-10-04 12:54:13,550', float('nan'),
                             '2023-10-04 12:55:13,550')
    except RMSConnectionCreationException as e:
        assert e.reason == 'connected'

    try:
        conn = RMSConnection('fdsa', 'fdsa', 'fdsa', 'fdsa', '2023-10-04 12:54:13,550', '2023-10-04 12:55:13,550',
                             float('nan'))
    except RMSConnectionCreationException as e:
        assert e.reason == 'leave'

    # RoomMeeting tests
    conn11 = RMSConnection('room_id', 'peer_id1', 'msid', 'rsid', '2023-10-04 12:54:13,550', '2023-10-04 12:55:13,550',
                           '2023-10-04 12:56:13,550')
    conn12 = RMSConnection('room_id', 'peer_id1', 'msid', 'rsid', '2023-10-04 12:56:30,000', '2023-10-04 12:56:35,000',
                           '2023-10-04 12:57:00,000')
    conn13 = RMSConnection('room_id', 'peer_id1', 'msid', 'rsid', '2023-10-04 12:54:13,550', '2023-10-04 12:55:13,550',
                           '2023-10-04 12:56:13,550')

    conn21 = RMSConnection('room_id', 'peer_id2', 'msid', 'rsid', '2023-10-04 12:54:13,550', '2023-10-04 12:55:13,550',
                           '2023-10-04 12:56:13,550')
    conn22 = RMSConnection('room_id', 'peer_id2', 'msid', 'rsid', '2023-10-04 12:54:13,550', '2023-10-04 12:55:13,550',
                           '2023-10-04 12:56:13,550')
    conn23 = RMSConnection('room_id', 'peer_id2', 'msid', 'rsid', '2023-10-04 12:54:13,550', '2023-10-04 12:55:13,550',
                           '2023-10-04 12:56:13,550')

    meetings = splitRoomMeetings({'room_id': [
        RMSConnection('room_id', 'peer_id1', 'msid', 'rsid', '2023-10-04 12:54:13,550', '2023-10-04 12:55:13,550',
                      '2023-10-04 12:56:00,000'),
        RMSConnection('room_id', 'peer_id1', 'msid', 'rsid', '2023-10-04 12:56:59,000', '2023-10-04 12:57:01,000',
                      '2023-10-04 12:58:00,000'),
        # first meeting ends
        RMSConnection('room_id', 'peer_id1', 'msid', 'rsid', '2023-10-04 12:59:00,000', '2023-10-04 12:59:05,000',
                      '2023-10-04 12:59:30,000'),

        RMSConnection('room_id', 'peer_id2', 'msid', 'rsid', '2023-10-04 12:59:30,000', '2023-10-04 12:59:31,000',
                      '2023-10-04 13:00:00,000'),
        # second meeting ends
        RMSConnection('room_id', 'peer_id2', 'msid', 'rsid', '2023-10-04 13:02:59,000', '2023-10-04 13:03:05,000',
                      '2023-10-04 13:05:00,000'),
        RMSConnection('room_id', 'peer_id2', 'msid', 'rsid', '2023-10-04 13:01:00,000', '2023-10-04 13:01:05,000',
                      '2023-10-04 13:02:00,000'),
        RMSConnection('room_id', 'peer_id2', 'msid', 'rsid', '2023-10-04 13:02:30,000', '2023-10-04 13:02:35,000',
                      '2023-10-04 13:02:50,000'),
    ]},
    meetingOnTheSameBridgeIdleTimeout
    )

    assert len(meetings) == 3
    assert meetings[0].ts_start == parseIsoDate('2023-10-04 12:54:13,550')
    assert meetings[0].ts_finish == parseIsoDate('2023-10-04 12:58:00,000')

    assert meetings[1].ts_start == parseIsoDate('2023-10-04 12:59:00,000')
    assert meetings[1].ts_finish == parseIsoDate('2023-10-04 13:00:00,000')

    assert meetings[1].peerConnections[0].ts_joined == parseIsoDate('2023-10-04 12:59:00,000')
    assert meetings[1].peerConnections[0].ts_leave == parseIsoDate('2023-10-04 12:59:30,000')
    assert meetings[1].peerConnections[1].ts_joined == parseIsoDate('2023-10-04 12:59:30,000')
    assert meetings[1].peerConnections[1].ts_leave == parseIsoDate('2023-10-04 13:00:00,000')

    assert meetings[2].ts_start == parseIsoDate('2023-10-04 13:01:00,000')
    assert meetings[2].ts_finish == parseIsoDate('2023-10-04 13:05:00,000')

    # RoomMeetingAssgnment tests

    db = RoomMeetingAssignments()
    rm1 = RoomMeeting([RMSConnection('room_id', 'peer_id2', 'msid', 'rsid', '2023-10-04 12:54:00,000',
                                     '2023-10-04 12:54:05,000', '2023-10-04 13:55:00,000')], meetingOnTheSameBridgeIdleTimeout)
    rm1.id = 'rm1'
    db.assignRoomMeeting(rm1, 0, rm1.ts_start)
    rm2 = RoomMeeting([RMSConnection('room_id', 'peer_id2', 'msid', 'rsid', '2023-10-04 13:54:00,000',
                                     '2023-10-04 13:54:05,000', '2023-10-04 13:55:01,000')], meetingOnTheSameBridgeIdleTimeout)
    rm2.id = "rm2"
    db.assignRoomMeeting(rm2, 0, rm2.ts_start)
    db.releaseRoomMeeting(rm1, rm1.ts_finish)
    db.releaseRoomMeeting(rm2, rm2.ts_finish)

    rm3 = RoomMeeting([RMSConnection('room_id', 'peer_id2', 'msid', 'rsid', '2023-10-04 14:54:00,000',
                                     '2023-10-04 14:54:05,000', '2023-10-04 14:55:00,000')], meetingOnTheSameBridgeIdleTimeout)
    rm3.id = "rm3"
    db.assignRoomMeeting(rm3, 0, rm3.ts_start)
    db.releaseRoomMeeting(rm3, rm3.ts_finish)

    rm4 = RoomMeeting([RMSConnection('room_id', 'peer_id2', 'msid', 'rsid', '2023-10-04 15:54:00,000',
                                     '2023-10-04 15:54:05,000', '2023-10-04 15:55:00,000')], meetingOnTheSameBridgeIdleTimeout)
    rm4.id = "rm4"
    db.assignRoomMeeting(rm4, 0, rm4.ts_start)
    db.releaseRoomMeeting(rm4, rm4.ts_finish)

    # print(str(db))

    #    nodeToRoomMeeting: dict[int, dict[datetime, list[RoomMeeting]]] = defaultdict(lambda :defaultdict(list))
    #    roomMeetingToNode: dict[str, dict[datetime, int]] = defaultdict(lambda: defaultdict(int))

    assert db.nodeToRoomMeeting == {
        0: {
            rm1.ts_start: [rm1.id],
            rm2.ts_start: [rm1.id, rm2.id],
            rm1.ts_finish: [rm2.id],
            rm2.ts_finish: [],
            rm3.ts_start: [rm3.id],
            rm3.ts_finish: [],
            rm4.ts_start: [rm4.id],
            rm4.ts_finish: [],
        }
    }


checker = 0

def testTraverser():
    global checker
    checker = 0

    def assertCheckerAndInc(desired: int):
        global checker
        assert desired == checker, f"desired checker {desired} is not equal to actual {checker}"
        checker += 1

    lists = [
        [
            RMSRestarterEvent(parseIsoDate('2023-10-04 15:54:00,000'), lambda: assertCheckerAndInc(0)),
            RMSRestarterEvent(parseIsoDate('2023-10-04 16:00:00,000'), lambda: assertCheckerAndInc(6))
        ],
        [
            RMSRestarterEvent(parseIsoDate('2023-10-04 15:55:00,000'), lambda: assertCheckerAndInc(1)),
            RMSRestarterEvent(parseIsoDate('2023-10-04 15:58:00,000'), lambda: assertCheckerAndInc(4)),
            RMSRestarterEvent(parseIsoDate('2023-10-04 15:59:00,000'), lambda: assertCheckerAndInc(5))
        ],
        [
            RMSRestarterEvent(parseIsoDate('2023-10-04 15:56:00,000'), lambda: assertCheckerAndInc(2)),
            RMSRestarterEvent(parseIsoDate('2023-10-04 15:57:00,000'), lambda: assertCheckerAndInc(3)),
            RMSRestarterEvent(parseIsoDate('2023-10-04 16:01:00,000'), lambda: assertCheckerAndInc(7))
        ],
        [
            RMSRestarterEvent(parseIsoDate('2023-10-04 16:02:00,000'), lambda: assertCheckerAndInc(8))
        ],
        [],
    ]

    MultiListTimestampTraverser(lists).traverse()
    print("MultiListTimestampTraverser check - success")

testTraverser()