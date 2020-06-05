"""General Kraft tests.

First as a setup for figuring out this sort of testing setup, maybe
develop some util functions.
"""

from kraft import *
from messages import *
from raftlog import LogEntry
from testutils import *


def test_get_later_term_update_and_step_down():
    config = gen_config(2)
    leader_id = 0
    s_id = 1
    s = RaftServer(s_id, config)._set_follower()._set_current_term(9)

    assert s.currentTerm == 9
    assert s.is_follower()
    send_message(s, Message(0, s_id, 11, AppendEntries(0, 1, 1, [], 1)))
    step_server(s)  # perform one step according to state
    # since log is empty AE will fail
    # but the term will still be updated
    assert s.currentTerm == 11
    assert s.is_follower()
    assert len(s.log) == 0

    send_message(
        s,
        Message(
            leader_id,
            s_id,
            12,
            AppendEntries(
                leader_id,
                index=0,
                prev_term=-1,
                entries=[LogEntry(12, 3)],
                leader_commit=1,
            ),
        ),
    )
    step_server(s)
    # AE succeeds, entries are added, AND term is updated
    assert s.currentTerm == 12
    assert s.is_follower()
    assert s.log == [LogEntry(12, 3)]

    l = RaftServer(leader_id, config)._set_leader()._set_current_term(9)

    assert l.currentTerm == 9
    assert l.is_leader()
    send_message(l, Message(leader_id, s_id, 13, AppendEntriesReply(False, -1, 4)))
    step_server(l)
    assert l.currentTerm == 13
    assert l.is_follower()

    c = RaftServer(s_id, config)._set_candidate()._set_current_term(9)

    assert c.currentTerm == 9
    assert c.is_candidate()
    send_message(c, Message(0, s_id, 11, AppendEntries(0, 1, 1, [], 1)))
    step_server(c)  # perform one step according to state
    # since log is empty AE will fail
    # but the term will still be updated and stepped down to follower
    assert c.currentTerm == 11
    assert c.is_follower()
    assert len(c.log) == 0
