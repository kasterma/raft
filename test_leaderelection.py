from collections import Counter

import pytest

from kraft import *
from messages import *
from testutils import *


def test_follower_interaction():
    config = gen_config(2)
    follower = RaftServer(0, config)._set_log(RaftLog.from_list([]))._set_follower()

    send_message(follower, Message(0, 1, 1, RequestVote(1, (1, 1))))
    step_server(follower)
    assert follower.currentTerm == 1
    assert follower.votedFor == 1
    m = get_message(follower)
    assert m.payload == RequestVoteReply(True)

    # candidate has outdated term
    follower.currentTerm = 2
    follower.votedFor = None

    send_message(follower, Message(0, 1, 1, RequestVote(1, (1, 1))))
    step_server(follower)
    assert follower.currentTerm == 2
    assert follower.votedFor == None
    m = get_message(follower)
    assert m.payload == RequestVoteReply(False)

    # follower has already given vote to this candidate
    follower.currentTerm = 0
    follower.votedFor = 1

    send_message(follower, Message(0, 1, 1, RequestVote(1, (1, 1))))
    step_server(follower)
    assert follower.currentTerm == 1
    assert follower.votedFor == 1
    m = get_message(follower)
    assert m.payload == RequestVoteReply(True)

    # follower has voted for other server
    follower.votedFor = 6

    send_message(follower, Message(0, 1, 1, RequestVote(1, (1, 1))))
    step_server(follower)
    assert follower.currentTerm == 1
    assert follower.votedFor == 6
    m = get_message(follower)
    assert m.payload == RequestVoteReply(False)

    # test requests with outdated log being rejected
    config = gen_config(2)
    follower = (
        RaftServer(0, config)
        ._set_log(RaftLog.from_list([LogEntry(3, 2)]))
        ._set_current_term(4)
        ._set_follower()
    )

    send_message(follower, Message(0, 1, 5, RequestVote(1, (1, 1))))
    step_server(follower)
    assert follower.votedFor is None
    msg = get_message(follower)
    assert msg.payload == RequestVoteReply(False)


def test_candidate_interaction():
    config = gen_config(3)
    candidate = (
        RaftServer(0, config)
        ._set_log(RaftLog([]))
        ._set_candidate()
        ._set_current_term(0)
    )

    # while candidate get message with newer term
    send_message(candidate, Message(1, 0, 1, AppendEntries(1, 0, -1, [], 0)))
    step_server(candidate)
    assert candidate.is_follower()
    assert candidate.currentTerm == 1
    msg = get_message(candidate)
    assert (
        msg is None
    )  # candidate could handle the appendEntries but we decided to drop

    candidate._set_candidate()

    send_message(candidate, Message(1, 0, 1, AppendEntries(1, 0, -1, [], 0)))
    step_server(candidate)
    assert candidate.is_candidate()
    assert sum(candidate.state.voteReceived.values()) == 1  # only vote for self
    ms = get_all_messages(candidate)
    assert set(
        aer.recver_id for aer in ms if isinstance(aer.payload, AppendEntriesReply)
    ) == {1}
    assert set(vr.recver_id for vr in ms if isinstance(vr.payload, RequestVote)) == {
        1,
        2,
    }

    send_message(candidate, Message(1, 0, 1, RequestVoteReply(True)))
    step_server(candidate)
    assert candidate.is_leader()

    config = gen_config(5)  # now need more votes
    candidate = RaftServer(0, config)._set_log(RaftLog([]))._set_candidate()

    send_message(candidate, Message(1, 0, 0, AppendEntries(1, 0, -1, [], 0)))
    step_server(candidate)
    ms = get_all_messages(candidate)
    assert candidate.is_candidate()
    assert sum(candidate.state.voteReceived.values()) == 1
    assert set(vr.recver_id for vr in ms if isinstance(vr.payload, RequestVote)) == {
        1,
        2,
        3,
        4,
    }

    send_message(candidate, Message(1, 0, 0, RequestVoteReply(True)))
    step_server(candidate)
    assert sum(candidate.state.voteReceived.values()) == 2
    assert candidate.is_candidate()
    ms = get_all_messages(candidate)
    assert set(vr.recver_id for vr in ms if isinstance(vr.payload, RequestVote)) == {
        2,
        3,
        4,
    }

    send_message(candidate, Message(1, 0, 0, RequestVoteReply(True)))
    step_server(candidate)
    assert sum(candidate.state.voteReceived.values()) == 2
    assert candidate.is_candidate()
    ms = get_all_messages(candidate)
    assert set(vr.recver_id for vr in ms if isinstance(vr.payload, RequestVote)) == {
        2,
        3,
        4,
    }

    send_message(candidate, Message(2, 0, 0, RequestVoteReply(True)))
    step_server(candidate)
    assert candidate.is_leader()
    ms = get_all_messages(candidate)
    assert len(ms) == 0


def test_leader_interaction():
    config = gen_config(3)
    leader = RaftServer(0, config)._set_log(RaftLog([]))._set_leader()

    # message of a new election
    send_message(leader, Message(2, 0, 1, RequestVoteReply(True)))
    step_server(leader)
    assert leader.currentTerm == 1
    assert leader.is_follower()
    ms = get_all_messages(leader)
    assert len(ms) == 0

    config = gen_config(3)
    leader = RaftServer(0, config)._set_log(RaftLog([]))._set_leader()

    # message of a new election
    send_message(leader, Message(2, 0, 1, RequestVote(True, (1, 1))))
    step_server(leader)
    assert leader.currentTerm == 1
    assert leader.is_follower()
    ms = get_all_messages(leader)
    assert len(ms) == 0

    config = gen_config(3)
    leader = (
        RaftServer(0, config)._set_log(RaftLog([]))._set_leader()._set_current_term(2)
    )

    # message of an old election
    send_message(leader, Message(2, 0, 1, RequestVote(True, (1, 1))))
    step_server(leader)
    assert leader.currentTerm == 2
    assert leader.is_leader()
    ms = get_all_messages(leader)
    assert len(ms) == 3
    assert ms[0] == Message(0, 2, 2, None)  # reply to the vote request
    assert ms[1] == Message(0, 1, 2, AppendEntries(0, 0, -1, [], 0))  # heartbeats
    assert ms[2] == Message(0, 2, 2, AppendEntries(0, 0, -1, [], 0))


def test_leader_election():
    server_ct = 3
    config = gen_config(server_ct)
    servers = [RaftServer(i, config) for i in range(server_ct)]

    assert all(server.is_follower() for server in servers)

    for server in servers:
        server.start()

    ct = 0
    while not any(server.is_leader() for server in servers) and ct < 10:
        ct += 1
        time.sleep(1)

    assert ct != 10  # should be done before 10 seconds

    states = [str(server.state) for server in servers]
    votes = [server.votedFor for server in servers]
    terms = [server.currentTerm for server in servers]
    assert Counter(states) == Counter(["Leader", "Follower", "Follower"])
    assert (
        len(set(votes)) == 1
    )  # Note: this test fails sometimes; election timeouts can trigger simultaneously  TODO: fix test
    assert set(terms) == {1}

    # check stable
    time.sleep(1)
    later_states = [str(server.state) for server in servers]
    later_votes = [server.votedFor for server in servers]
    later_terms = [server.currentTerm for server in servers]

    assert states == later_states
    assert votes == later_votes
    assert terms == later_terms

    for server in servers:
        server.stop()


def test_start_with_chosen_leader():
    server_ct = 3
    config = gen_config(server_ct)
    servers = [RaftServer(i, config) for i in range(server_ct)]

    assert all(server.is_follower() for server in servers)
    servers[0]._set_leader()

    for server in servers:
        server.start()

    time.sleep(2)

    states = [str(server.state) for server in servers]
    votes = [server.votedFor for server in servers]
    terms = [server.currentTerm for server in servers]
    assert Counter(states) == Counter(["Leader", "Follower", "Follower"])
    assert set(terms) == {0}

    for server in servers:
        server.stop()
