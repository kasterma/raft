import logging
import random
import time
from logging.config import dictConfig

import pytest
import yaml

from kraft import *
from messages import *
from raftlog import LogEntry, RaftLog
from testutils import *


def test_follower_interaction():
    """Note: this test does look at the internals of the follower.  This
    could possibly be avoided, but the gains of that are not clear to
    me yet.

    """
    config = gen_config(3)
    follower_id = 1
    leader_id = 0
    follower = (
        RaftServer(follower_id, config)._set_log(RaftLog.from_list([]))._set_follower()
    )

    assert follower.currentTerm == 0
    assert follower.commitIndex == 0
    assert follower.leaderId is None
    send_message(
        follower,
        Message(
            leader_id,
            follower_id,
            1,
            AppendEntries(leader_id, 0, -1, [LogEntry(1, 1)], 0),
        ),
    )
    step_server(follower)
    assert follower.log == [LogEntry(1, 1)]
    assert follower.currentTerm == 1
    assert follower.commitIndex == 0
    assert follower.leaderId == leader_id
    assert get_message(follower).payload == AppendEntriesReply(True, 1, -1)

    ## checking idempotence; doing the above again
    send_message(
        follower,
        Message(
            leader_id,
            follower_id,
            1,
            AppendEntries(leader_id, 0, -1, [LogEntry(1, 1)], 0),
        ),
    )
    step_server(follower)
    assert follower.log == [LogEntry(1, 1)]
    assert follower.currentTerm == 1
    assert follower.commitIndex == 0
    assert follower.leaderId == leader_id
    assert get_message(follower).payload == AppendEntriesReply(True, 1, -1)

    # message indicating that the log entry is now committed
    send_message(
        follower,
        Message(leader_id, follower_id, 1, AppendEntries(leader_id, 1, 1, [], 1)),
    )
    step_server(follower)
    assert follower.log == [LogEntry(1, 1)]
    assert follower.currentTerm == 1
    assert follower.commitIndex == 1  # this is the update
    assert follower.leaderId == leader_id
    assert get_message(follower).payload == AppendEntriesReply(True, 1, -1)

    # now lengthen the log; one new entry is already commited the other not yet
    send_message(
        follower,
        Message(
            leader_id,
            follower_id,
            1,
            AppendEntries(leader_id, 1, 1, [LogEntry(1, 2), LogEntry(1, 3)], 2),
        ),
    )
    step_server(follower)
    assert follower.log == [LogEntry(1, 1), LogEntry(1, 2), LogEntry(1, 3)]
    assert follower.currentTerm == 1
    assert follower.commitIndex == 2
    assert follower.leaderId == leader_id  # and hence has been updated
    assert get_message(follower).payload == AppendEntriesReply(True, 3, -1)

    # new leader, makes some changes to the log
    leader_id = 2
    send_message(
        follower,
        Message(
            leader_id,
            follower_id,
            2,
            AppendEntries(leader_id, 2, 1, [LogEntry(2, 3)], 3),
        ),
    )
    step_server(follower)
    assert follower.log == [LogEntry(1, 1), LogEntry(1, 2), LogEntry(2, 3)]
    assert follower.currentTerm == 2
    assert follower.commitIndex == 3
    assert get_message(follower).payload == AppendEntriesReply(True, 3, -1)

    # send append entries from leader with old term
    leader_id = 2
    send_message(
        follower,
        Message(
            leader_id,
            follower_id,
            0,
            AppendEntries(leader_id, 2, 1, [LogEntry(2, 3)], 3),
        ),
    )
    step_server(follower)
    assert follower.log == [LogEntry(1, 1), LogEntry(1, 2), LogEntry(2, 3)]
    assert follower.currentTerm == 2
    assert follower.commitIndex == 3
    assert get_message(follower).payload == AppendEntriesReply(False, -1, 2)


def test_leader_interaction():
    """Note: this test does look at the internals of the leader.  This
    could possibly be avoided, but the gains of that are not clear to
    me yet.

    """
    config = gen_config(3)
    follower_ids = [1, 2]
    leader_id = 0
    leader = (
        RaftServer(leader_id, config)
        ._set_log(RaftLog.from_list([LogEntry(1, 1)]))
        ._set_leader()
    )

    assert all(leader.state.nextIndex[i] == 1 for i in follower_ids)
    assert all(leader.state.matchIndex[i] == 0 for i in follower_ids)
    step_server(leader)
    ms = get_all_messages(leader)
    assert [m.recver_id for m in ms] == follower_ids
    assert all(m.payload == AppendEntries(leader_id, 1, 1, [], 0) for m in ms)
    assert all(leader.state.matchIndex[i] == 0 for i in follower_ids)
    assert all(leader.state.nextIndex[i] == 1 for i in follower_ids)

    # send to everyone again
    step_server(leader)
    ms = get_all_messages(leader)
    assert [m.recver_id for m in ms] == follower_ids

    send_message(leader, Message(1, 0, 0, AppendEntriesReply(False, -1, 1)))
    step_server(leader)
    ms = get_all_messages(leader)
    assert [m.recver_id for m in ms] == follower_ids
    assert all(leader.state.matchIndex[i] == 0 for i in follower_ids)
    assert leader.state.nextIndex[1] == 0
    assert leader.state.nextIndex[2] == 1

    msd = {m.recver_id: m.payload for m in ms}
    assert msd[1] == AppendEntries(leader_id, 0, -1, [LogEntry(1, 1)], 0)
    assert msd[2] == AppendEntries(leader_id, 1, 1, [], 0)

    send_message(leader, Message(1, 0, 0, AppendEntriesReply(True, 1, -1)))
    step_server(leader)
    ms = get_all_messages(leader)
    assert [m.recver_id for m in ms] == [
        1,
        2,
    ]  # 1 now synched, but send empty appendEntries
    assert leader.state.matchIndex[1] == 1
    assert leader.state.matchIndex[2] == 0
    assert leader.state.nextIndex[1] == 1
    assert leader.state.nextIndex[2] == 1

    # assume leader had some entries added
    leader.log.append_entries(1, 1, [LogEntry(1, 2), LogEntry(1, 3)])
    assert leader.log == [LogEntry(1, 1), LogEntry(1, 2), LogEntry(1, 3)]
    step_server(leader)
    ms = get_all_messages(leader)
    assert [m.recver_id for m in ms] == follower_ids
    assert leader.state.matchIndex[1] == 1
    assert leader.state.matchIndex[2] == 0
    msd = {m.recver_id: m.payload for m in ms}
    assert msd[1] == AppendEntries(leader_id, 1, 1, [LogEntry(1, 2), LogEntry(1, 3)], 0)
    # leader still has not heard from 2, so nextIndex not yet lowered
    assert msd[2] == AppendEntries(leader_id, 1, 1, [LogEntry(1, 2), LogEntry(1, 3)], 0)

    # tell leader we can't add the entry at index 1
    send_message(leader, Message(2, 0, 0, AppendEntriesReply(False, -1, 1)))
    step_server(leader)
    ms = get_all_messages(leader)
    assert [m.recver_id for m in ms] == follower_ids
    assert leader.state.matchIndex[1] == 1
    assert leader.state.matchIndex[2] == 0
    msd = {m.recver_id: m.payload for m in ms}
    print(msd)
    assert msd[1] == AppendEntries(leader_id, 1, 1, [LogEntry(1, 2), LogEntry(1, 3)], 0)
    # leader still has not heard from 2, so nextIndex not yet lowered
    assert msd[2] == AppendEntries(
        leader_id, 0, -1, [LogEntry(1, 1), LogEntry(1, 2), LogEntry(1, 3)], 0
    )


@pytest.mark.parametrize(
    "log_leader, log_follower",
    [
        (
            RaftLog.from_list([LogEntry(1, 1), LogEntry(2, 2), LogEntry(3, 3)]),
            RaftLog.from_list([]),
        ),
        (
            RaftLog.from_list([LogEntry(1, 1), LogEntry(2, 2), LogEntry(3, 3)]),
            RaftLog.from_list([LogEntry(1, 1), LogEntry(1, 2), LogEntry(1, 2)]),
        ),
        (
            RaftLog.from_list([LogEntry(1, 1), LogEntry(2, 2)]),
            RaftLog.from_list(
                [LogEntry(1, 1), LogEntry(1, 2), LogEntry(1, 2), LogEntry(1, 3)]
            ),
        ),
        (RaftLog.from_list([]), RaftLog.from_list([]),),
    ],
)
def test_basic_replication(log_leader, log_follower):
    p = random.randint(
        10000, 20000
    )  # randomly chosen port; should really check are unused
    config = {1: {"port": p, "dir": None}, 2: {"port": p + 1, "dir": None}}
    leader = RaftServer(1, config)._set_log(log_leader)._set_leader()._set_notimeouts()
    follower = (
        RaftServer(2, config)._set_follower()._set_notimeouts()._set_log(log_follower)
    )
    leader.start()
    follower.start()

    while not leader._fully_committed():
        time.sleep(0.1)

    follower.stop()
    leader.stop()

    assert follower.log == log_leader


@pytest.mark.parametrize(
    "log_leader, log_follower1, log_follower2",
    [
        (
            RaftLog.from_list([LogEntry(1, 1), LogEntry(2, 2), LogEntry(3, 3)]),
            RaftLog.from_list([]),
            RaftLog.from_list([LogEntry(1, 1), LogEntry(1, 2), LogEntry(1, 2)]),
        ),
        (
            RaftLog.from_list([LogEntry(1, 1), LogEntry(2, 2), LogEntry(3, 3)]),
            RaftLog.from_list([LogEntry(1, 1), LogEntry(1, 2), LogEntry(1, 2)]),
            RaftLog.from_list([LogEntry(1, 1), LogEntry(1, 2), LogEntry(1, 2)]),
        ),
        (
            RaftLog.from_list([LogEntry(1, 1), LogEntry(2, 2)]),
            RaftLog.from_list(
                [LogEntry(1, 1), LogEntry(1, 2), LogEntry(1, 2), LogEntry(1, 3)]
            ),
            RaftLog.from_list([]),
        ),
        (RaftLog.from_list([]), RaftLog.from_list([]), RaftLog.from_list([])),
    ],
)
def test_mutliple_replication(log_leader, log_follower1, log_follower2):
    p = random.randint(
        10000, 20000
    )  # randomly chosen port; should really check are unused
    config = {
        1: {"port": p, "dir": None},
        2: {"port": p + 1, "dir": None},
        3: {"port": p + 2, "dir": None},
    }
    leader = RaftServer(1, config)._set_log(log_leader)._set_leader()._set_notimeouts()
    follower1 = (
        RaftServer(2, config)._set_follower()._set_notimeouts()._set_log(log_follower1)
    )
    follower2 = (
        RaftServer(3, config)._set_follower()._set_notimeouts()._set_log(log_follower2)
    )
    leader.start()
    follower1.start()
    follower2.start()

    ct = 0
    while not leader._fully_committed() and ct < 4:
        print(f"leader.log {repr(leader.log)}.")
        print(f"follower1.log {repr(follower1.log)}.")
        print(f"follower2.log {repr(follower2.log)}.")
        time.sleep(1)
        ct += 1

    follower2.stop()
    follower1.stop()
    leader.stop()

    assert follower2.log == log_leader
    assert follower1.log == log_leader
