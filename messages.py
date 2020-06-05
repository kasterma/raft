from dataclasses import dataclass
from typing import List, Tuple, Union

from machine import Command
from raftlog import LogEntry


@dataclass
class Message:
    # headers
    sender_id: int
    recver_id: int
    sender_term: int

    payload: object


@dataclass
class AppendEntries:
    leader_id: int
    index: int  # index to use in log.append, i.e. the index where the first elt of entries needs to go
    prev_term: int  # the term of the entry at index-1 (or ignored if index=0)
    entries: List[LogEntry]
    leader_commit: int  # index up to where the leader is committed


@dataclass
class AppendEntriesReply:
    success: bool
    log_length: int  # length of the log after the append_entries succeeded (this is all the info needed to update leader state)
    index: int  # the index from the AppendEntries message that failed


@dataclass
class RequestVote:
    candidate_id: int
    log_measure: Tuple[
        int, int
    ]  # last term , lenght log; to be directly compared with log.measure_log()


@dataclass
class RequestVoteReply:
    vote_granted: bool


@dataclass
class ClientRequest:
    client_id: str  # uuid of some such
    cmd: Command


# Note: can have both leader_id and val set to None if the server is
# not leader but also doesn't know who is (e.g. after recent restart)
@dataclass
class ClientResponse:
    cmd_id: str  # the id of the command this was a response to
    leader_id: Union[None, int]  # initially don't know who the leader is
    val: Union[None, int]
