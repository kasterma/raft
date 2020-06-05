import logging
import pdb
import queue
import random
import socket
import sys
import threading
import time
from dataclasses import dataclass
from logging.config import dictConfig
from queue import Queue
from typing import List, Tuple

import click
from ruamel.yaml import YAML

from expirytimer import ExpiryTimer
from filteringqueue import FilteringQueue
from machine import StateMachine
from messages import *
from metrics import metrics
from osocket import ClientObjectSocket, COSConnectionClosed
from raftlog import LogEntry, RaftLog


class State:
    logger = logging.getLogger("state")

    def _log(self, s, log_fn, *args, **kwargs):
        """s is the format string, log_fn selects the log level"""
        log_fn("%s: " + s, self.server_str, *args, **kwargs)

    def linfo(self, s: str, *args, **kwargs):
        self._log(s, self.logger.info, *args, **kwargs)

    def ldebug(self, s: str, *args, **kwargs):
        self._log(s, self.logger.debug, *args, **kwargs)

    def lwarning(self, s: str, *args, **kwargs):
        self._log(s, self.logger.warning, *args, **kwargs)

    def __init__(self, server: "RaftServer"):
        self.server = server
        self.expiry_timer = ExpiryTimer(1000)
        self.send_messages = True

    def __getattr__(self, *args, **kwargs):
        """allows calling self.f() when mean self.server.f()

        What could go wrong?
        """
        return getattr(self.server, *args, **kwargs)

    def step(self):
        raise NotImplementedError()

    def to_follower(self):
        raise NotImplementedError()

    def to_leader(self):
        raise NotImplementedError()

    def to_candidate(self):
        raise NotImplementedError()

    def __repr__(self):
        return self.__class__.__name__

    def message(self, recver_id, payload):
        return Message(self.id, recver_id, self.currentTerm, payload)

    def send_and_log_message(self, recver_id, payload, log_str, *log_args):
        try:
            self.sendQueue.put(
                self.message(recver_id, payload), block=False,
            )
            self.ldebug(log_str, *log_args)
        except queue.Full:
            self.ldebug("dropped message %s to %d", payload, recver_id)

    def send_and_log_empty_message(self, recver_id):
        self.send_and_log_message(
            recver_id, None, "sending empty message to %d", recver_id
        )

    def request_vote(self, recver_id):
        self.send_and_log_message(
            recver_id,
            RequestVote(self.id, self.log.measure_log()),
            "vote request to %d",
            recver_id,
        )

    def request_vote_reply(self, request: Message, result):
        self.send_and_log_message(
            request.sender_id,
            RequestVoteReply(result),
            "%s for %d",
            "voted" if result else "didn't vote",
            request.payload.candidate_id,
        )

    def append_entries(self, recver_id):
        index = self.nextIndex[recver_id]
        prev_term = -1 if index == 0 else self.log[index - 1].term
        self.send_and_log_message(
            recver_id,
            AppendEntries(
                self.id, index, prev_term, self.log[index:], self.commitIndex,
            ),
            "send append_entries to %d",
            recver_id,
        )

    def append_entries_reply(self, request, result):
        self.send_and_log_message(
            request.sender_id,
            AppendEntriesReply(
                result,
                len(self.log) if result else -1,
                -1 if result else request.payload.index,
            ),
            "append_entries_reply %s",
            result,
        )

    def client_not_leader_reply(self, request):
        self.send_and_log_message(
            request.sender_id,
            ClientResponse(request.payload.cmd.id, self.leaderId, None),
            "replied not leader to %s",
            request.sender_id,
        )

    def client_reply(self, request, val):
        self.send_and_log_message(
            request.sender_id,
            ClientResponse(request.payload.cmd.id, self.leaderId, val),
            "replied %s to %s",
            str(val),
            request.sender_id,
        )


class Leader(State):
    logger = logging.getLogger("leader")

    def __init__(self, server: "RaftServer"):
        super().__init__(server)
        self.nextIndex = {i: len(self.log) for i in self.config.keys()}
        self.matchIndex = {i: 0 for i in self.config.keys()}

    def all_in_sync(self):
        self.ldebug(
            f"fully committed matchIndex {self.matchIndex}:{self.nextIndex} len {len(self.server.log)}."
        )
        for id, match in self.matchIndex.items():
            if id != self.id:  # not updated for leader itself
                if match == len(self.log):
                    continue
                self.ldebug("match failed on id %d", id)
                return False
        return True

    @metrics(tag="leader")
    def step(self):
        self.ldebug("step start")
        # deal with some messages from the received queue
        incoming_msg_count = 0
        try:
            for _ in range(10):  # handle next (up to) 10 messages
                m: Message = self.server.recvQueue.get(block=False)
                incoming_msg_count += 1
                self.ldebug("received message %s", m)

                if isinstance(m.payload, AppendEntriesReply):
                    if self.currentTerm < m.sender_term:
                        self.to_follower(m.sender_term)
                        return
                    if m.payload.success:
                        self.matchIndex[m.sender_id] = m.payload.log_length
                        self.nextIndex[m.sender_id] = m.payload.log_length
                        self.ldebug("AE success handled")
                    else:
                        self.nextIndex[m.sender_id] = max(0, m.payload.index - 1)
                        self.ldebug("nextIndex down one %s", m)
                elif isinstance(m.payload, ClientRequest):
                    v = self.machine.lookup(m.payload.cmd)
                    if not v:
                        self.log.add_entry(LogEntry(self.currentTerm, m.payload.cmd))
                        self.matchIndex[self.id] = len(self.log)
                    self.ldebug("clientrequest")
                    self.client_reply(m, v)
                else:
                    if self.currentTerm < m.sender_term:
                        self.to_follower(m.sender_term)
                        return
                    elif m.sender_term < self.currentTerm:
                        self.send_and_log_empty_message(
                            m.sender_id
                        )  # update sender term
                    self.ldebug("dropping %s not a message for leader.", m)
        except queue.Empty:
            self.ldebug(
                "handled %d incomming messages.", incoming_msg_count,
            )

        # increase commit index
        while (
            len([i for i, v in self.matchIndex.items() if v > self.commitIndex])
            > len(self.config) / 2
        ):
            self.server.commitIndex += 1

        for idx in range(self.lastApplied + 1, self.commitIndex):
            self.linfo("applying instruction {self.log[idx]}.")
            self.machine.execute(self.log[idx].item)
            self.server.lastApplied = idx

        # send all AE messages that need to be send
        if self.send_messages:
            self.ldebug("sending")
            for i in self.config.keys():
                if i != self.id:
                    self.append_entries(i)

        self.ldebug("step end")

    def to_follower(self, new_term):
        """Step down to follower; always happens when note a later term is active"""
        self.linfo(
            "stepping down b/c term change %d -> %d.", self.currentTerm, new_term,
        )
        self.update_current_term(new_term)
        self.server.state = Follower(self.server)


class Follower(State):
    logger = logging.getLogger("follower")

    @metrics(tag="follower")
    def step(self):
        self.ldebug("step on follower")
        # handle a message in the received queue
        try:
            m = self.recvQueue.get(block=False)
            self.ldebug("receivded message %s", m)

            if isinstance(m.payload, AppendEntries):
                ae: AppendEntries = m.payload
                if self.currentTerm > m.sender_term:
                    self.append_entries_reply(m, False)
                else:
                    self.update_current_term(m.sender_term)
                    self.server.leaderId = m.payload.leader_id
                    self.expiry_timer.reset()
                    success = self.log.append_entries(
                        m.payload.index, m.payload.prev_term, m.payload.entries
                    )
                    self.append_entries_reply(m, success)
                    if success:
                        self.server.commitIndex = (
                            m.payload.leader_commit
                        )  # to be able to apply committed entries to state machine
            elif isinstance(m.payload, RequestVote):
                self.ldebug("recv request to vote")
                if self.currentTerm > m.sender_term:  # candidate has old term
                    self.request_vote_reply(m, False)
                else:
                    if self.currentTerm < m.sender_term:
                        self.update_current_term(m.sender_term)
                    if (
                        self.votedFor is None or self.votedFor == m.payload.candidate_id
                    ) and self.log.measure_log() <= m.payload.log_measure:
                        # vote is available (or repeat) and candidate has good enough log
                        self.server.votedFor = m.payload.candidate_id
                        self.request_vote_reply(m, True)
                    else:
                        self.request_vote_reply(m, False)

            elif isinstance(m.payload, ClientRequest):
                self.ldebug("reply not leader")
                self.client_not_leader_reply(m)
            else:
                if self.currentTerm < m.sender_term:
                    self.update_current_term(m.sender_term)
                    self.ldebug("updated term")

                self.ldebug("not a follower message %s", m)
        except queue.Empty:  # is fine there are no messages to deal with
            self.ldebug("empty queue")

        if self.expiry_timer.expired():
            self.to_candidate()

        self.ldebug("step end")

    def to_candidate(self):
        self.linfo("stepping up to candidate.")
        self.server.state = Candidate(self.server)


class Candidate(State):
    logger = logging.getLogger("candidate")

    def __init__(self, server):
        super().__init__(server)
        self.server.currentTerm += 1
        self.voteReceived = {i: False for i in self.config.keys()}
        self.voteReceived[self.id] = True

    @metrics(tag="candidate")
    def step(self):
        self.ldebug("step start")
        # handle some messages in the received queue
        msg_count = 0
        try:
            for _ in range(10):
                m = self.recvQueue.get(block=False)
                msg_count += 1
                self.ldebug("received message %s", m)

                if isinstance(m.payload, AppendEntries):
                    if self.currentTerm >= m.sender_term:
                        # the server we send this message to (if it gets it) will step down as leader
                        self.append_entries_reply(m, False)
                    else:
                        self.update_current_term(m.sender_term)
                        self.to_follower()  # we could perform the AE; but the follower will do it on resend
                        self.ldebug("step down to follower.")
                        return
                elif isinstance(m.payload, RequestVote):
                    # another server is trying to become leader
                    if self.currentTerm >= m.sender_term:
                        self.request_vote_reply(m, False)
                    else:
                        self.update_current_term(m.sender_term)
                        self.to_follower()
                        self.ldebug("step down to follower.")
                        return
                elif isinstance(m.payload, RequestVoteReply):
                    if self.currentTerm < m.sender_term:
                        self.update_current_term(m.sender_term)
                        self.to_follower()
                        self.ldebug("step down to follower.")
                        return
                    elif m.payload.vote_granted:
                        self.voteReceived[m.sender_id] = True
                    else:  # vote not granted, this server already voted for another candidate
                        pass
                elif isinstance(m.payload, ClientRequest):
                    self.client_not_leader_reply(m)
                else:
                    if self.currentTerm < m.sender_term:
                        self.update_current_term(m.sender_term)
                        self.to_follower()
                        self.ldebug("updated term")

                    self.ldebug("not a candidate message %s", m)
        except queue.Empty:
            self.ldebug("handled %d messages", msg_count)

        self.ldebug("checking if have majority")
        if sum(self.voteReceived.values()) > len(self.voteReceived.keys()) / 2:
            self.to_leader()
            self.ldebug("step up to leader.")
            return

        self.ldebug("sending to servers that have not voted yet")
        if self.send_messages:
            for id in [id for id, vote in self.voteReceived.items() if not vote]:
                self.request_vote(id)
        self.ldebug("step end")

    def to_follower(self):
        self.linfo("step down to follower")
        self.server.leaderId = None
        self.server.state = Follower(self.server)

    def to_leader(self):
        # don't change self.votedFor
        self.linfo("step up to leader")
        self.server.leaderId = self.id
        self.server.state = Leader(self.server)  # TODO: what else to update


class RaftServer:
    logger = logging.getLogger("raftserver")
    logger_isRunning = logging.getLogger("isrunning")

    def __init__(self, id, config, queue_maxsize=10):
        self.logger.info(
            f"Setting up raft server with id {id}, configuration {config}."
        )
        directory = pathlib.Path(config[id]["dir"]) if config[id]["dir"] else None
        # persistent (log is internally persisted, so just need to persist the others)
        # Note: the log assumes all integer named files are log entries
        if directory and not directory.is_dir():
            directory.mkdir()
        self.config_file = directory / "server-config.yaml" if directory else None
        if self.config_file and self.config_file.is_file():
            with open(self.config_file) as f:
                config_from_file = yaml.load(f)
            if config_from_file["config"] != config or config_from_file["id"] != id:
                raise RaftServerInconsistentConfigError(config_from_file, id, config)
        else:
            config_from_file = {}
        self.id = config_from_file.get("id", id)
        self.config = config_from_file.get("config", config)
        self.currentTerm = config_from_file.get("currentTerm", 0)
        self.votedFor = config_from_file.get("votedFor", None)
        self.persist()

        self.log = (
            RaftLog(directory) if directory else RaftLog()
        )  # persists itself; but by above dir has been created

        # volatile
        self.commitIndex = 0
        self.lastApplied = -1
        self.state = Follower(self)
        self.leaderId = None

        self.clientsockets = {i: None for i, _ in self.config.items()}
        self.sendQueue = Queue(queue_maxsize)
        self.recvQueue = Queue(queue_maxsize)

        self.isRunning = True
        self.lock = threading.Lock()

        self.server_str = f"RaftServer({self.id})"
        self.slow_step = 0

        self.machine = StateMachine()

    def persist(self):
        """the log is already persisted, so only work on config"""
        if self.config_file:
            config_to_file = {
                "id": self.id,
                "config": self.config,
                "currentTerm": self.currentTerm,
                "votedFor": self.votedFor,
            }
            with open(self.config_file, "w") as f:
                yaml.dump(config_to_file, f)

    def update_current_term(self, new_term):
        if new_term < self.currentTerm:
            raise RuntimeError()
        self.votedFor = None
        self.currentTerm = new_term

    def _isRunning(self, msg):
        self.logger_isRunning.debug("Check _isRunning %s", msg)
        with self.lock:
            return self.isRunning

    def start(self):
        self.startReceiving()
        self.startSending()

        def run_server():
            while self._isRunning(f"{self.server_str}-main"):
                self.state.step()
                if self.slow_step:
                    time.sleep(self.slow_step)

        threading.Thread(target=run_server, name=f"{self.server_str}-main").start()
        return self

    def stop(self):
        """Stop the send and recv threads.

        Note: this sets the flags they check, still need the different
        socket timeouts to happen.

        """
        with self.lock:
            self.isRunning = False
        self.logger_isRunning.info(f"RaftServer({self.id}): isRunning now false.")
        return self

    def startReceiving(self):
        """Start the server socket and listening on it.

        On every incoming connection, retrieve all objects from the connection.  These
        objects are put in the recvQueue.
        """
        self.logger.info(f"Start receiving on {self.id}")
        server = ClientObjectSocket.get_cosserver(self.config[self.id]["port"])

        def listen_on_connection(s):
            # if the listensocket closes or has error, just drop it
            try:
                while self._isRunning("listen_on_connection"):
                    try:
                        received_object = s.recv_object()
                        # store socket to client to be able to reply
                        if isinstance(received_object.payload, ClientRequest):
                            self.clientsockets[received_object.sender_id] = s
                        self.recvQueue.put(received_object, block=False)
                    except socket.timeout:
                        continue  # expected timeout; needed to check if recvRunning is still true
                    except queue.Full:
                        self.logger.debug(
                            f"RaftServer({self.id}): recvQueue is full dropping message {received_object}."
                        )
                    self.logger.debug(
                        f"Server {self.id} received object {received_object}"
                    )
            except COSConnectionClosed as e:
                s.close()
                self.logger.debug("listening socket closed")
            except Exception as e:
                self.logger.debug(
                    f"Server {self.id}: Unexpected exception in listedn_on_connection_fn: {e}."
                )
                raise

        def listen_for_connections():
            while self._isRunning("listen_for_connections"):
                try:
                    (client, addr) = server.accept()
                    self.logger.debug("Connection from %s", addr)
                    threading.Thread(
                        target=listen_on_connection,
                        args=(client,),
                        name=f"RaftServer({self.id}): listen_on_connection",
                    ).start()
                except socket.timeout:
                    continue  # expected timeout; needed to check if sendRunning is still true
                except:
                    self.logger.debug(
                        "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"
                    )

        self.listening_thread = threading.Thread(
            target=listen_for_connections,
            name=f"RaftServer({self.id}): listen_for_connections.",
        )
        self.listening_thread.start()
        self.logger.info("Started server thread for server %d", self.id)

    def startSending(self):
        def sendfn():
            while self._isRunning("sendfn"):
                try:
                    message = self.sendQueue.get(timeout=1)
                    self.logger.debug(
                        "Server %d send %s to %d",
                        message.sender_id,
                        repr(message),
                        message.recver_id,
                    )
                    # only open sockets for other servers not for clients
                    if (
                        self.clientsockets.get(message.recver_id) is None
                        and message.recver_id in self.config.keys()
                    ):
                        self.clientsockets[
                            message.recver_id
                        ] = ClientObjectSocket.get_cosclient(
                            self.config[message.recver_id]["port"]
                        )
                    self.clientsockets[message.recver_id].send_object(message)
                except BrokenPipeError as e:
                    self.logger.debug(
                        f"Dropping message {message} to {message.recver_id}; connection was closed: {e}."
                    )
                    self.clientsockets[dest].close()
                    del self.clientsockets[dest]
                except ConnectionRefusedError as e:
                    self.logger.debug(
                        f"Dropping message {message} to {message.recver_id}; connection was refused: {e}."
                    )
                except queue.Empty:
                    continue  # expected empty; keep checking the queue
                except socket.timeout:
                    continue  # expected timeout; needed to check if sendRunning is still true
                except:
                    self.logger.debug(
                        "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
                    )

        threading.Thread(target=sendfn, name=f"RaftServer({self.id}): sendfn.").start()

    def start_sending(self):
        """send random messages to random servers (only for testing)"""

        def randomsend_fn():
            for idx in range(5):
                time.sleep(1)
                dest = random.choice(list(self.clientsockets.keys()))
                self.logger.debug("send %d to %d", idx, dest)
                o = Message(self.id, dest, self.currentTerm, idx)
                try:
                    self.sendQueue.put(o, block=False)
                except queue.Full:
                    self.logger.debug(f"sendQueue is full dropping message {o}.")

        threading.Thread(
            target=randomsend_fn, name=f"RaftServer({self.id}): random_sender."
        ).start()

    def is_follower(self):
        return isinstance(self.state, Follower)

    def is_leader(self):
        return isinstance(self.state, Leader)

    def is_candidate(self):
        return isinstance(self.state, Candidate)

    def _set_leader(self):
        """Make self be a leader"""
        self.state = Leader(self)
        self.leaderId = self.id
        self.votedFor = (
            self.id
        )  # become leader by first being candidate, a candidate votes for themselves
        return self

    def _set_follower(self):
        """Make self be a follower"""
        self.state = Follower(self)
        return self

    def _set_candidate(self):
        """Make self a candidate"""
        self.state = Candidate(self)
        return self

    def _set_current_term(self, t):
        self.currentTerm = t
        return self

    def _set_notimeouts(self):
        """Make self never have election timeouts fire"""
        self.state.expiry_timer._set_noexpire()
        return self

    def _set_log(self, log):
        self.log = log
        return self

    def _set_no_sending(self, sending=False):
        """On leader turns of the sending of heartbeats, on candidate turns of the sending of vote requests.
        The servers will still send replies.

        """
        self.state.send_messages = sending
        return self

    def _set_slow_step(self, slow=0.3):
        self.slow_step = slow
        return self

    def _fully_committed(self):
        """Check if this is the leader and it believes everything is commited"""
        self.logger.debug("check fully commited")
        if isinstance(self.state, Leader):
            rv = self.state.all_in_sync()
            self.logger.debug(f"fully committed {rv}.")
            return rv
        else:
            self.logger.debug(f"not a leader but {self.state}.")
        return False


class RaftNetwork:
    logger = logging.getLogger("raftnetwork")

    def __init__(self, config):
        self.config = config
        self.logger.info(f"Setting up raft network with configuration {self.config}")
        self.servers = [RaftServer(id, config) for id in config.keys()]

    def start(self):
        """Start the sending and recieving of messages"""
        for server in self.servers:
            server.start()

    def stop(self):
        for server in self.servers:
            server.stop()

    def setup_filtering_queues(self):
        for server in self.servers:
            server.recvQueue = FilteringQueue()
            server.sendQueue = FilteringQueue()

    def add_filter(self, drop_filter_fn):
        for server in self.servers:
            server.recvQueue.update_filter(drop_filter_fn=drop_filter_fn)
            server.sendQueue.update_filter(drop_filter_fn=drop_filter_fn)

    def startsending(self):
        """send random messages between the nodes (only for testing)"""
        for server in self.servers:
            server.start_sending()


def print_all_threads():
    for t in threading.enumerate():
        print(t)


def keep_showing_active_threads():
    def print_active_threads(sleep_time):
        while True:
            print_all_threads()
            time.sleep(sleep_time)

    threading.Thread(
        target=print_active_threads, args=(5,), name="threadprinter"
    ).start()


@click.group()
@click.option("--logging", default="logging.yaml")
def cli(logging):
    with open(logging) as f:
        dictConfig(YAML().load(f))


@cli.command()
@click.option("--config", default="config.yaml")
def client(config):
    with open(config) as f:
        config = YAML().load(f)
    print(f"starting client {config}")


@cli.command()
@click.option("--config", default="config.yaml")
@click.option("--start-only", type=click.INT, default=-1)
def start(config, start_only):
    with open(config) as f:
        config = YAML().load(f)
    print(f"starting the process with all raft servers {config}")
    if start_only < 0:
        net = RaftNetwork(config)
        net.setup_filtering_queues()
        net.add_filter(drop_filter_fn=lambda m: m.recver_id == 1)
        net.start()
        net.startsending()
        print("xyxyx")

        time.sleep(10)
        net.stop()
    else:
        server = RaftServer(start_only, config)
        server.start_sending()


@cli.command()
@click.option("--config", default="config.yaml")
@click.option("--count", type=click.INT, default=3)
@click.option("--basedir", default=None)
def config(config, count, basedir):
    """Generate a raft cluster config.

    We expect it will be useful to be able to switch quickly when ports are
    not closed yet waiting for a timeout.
    """
    print("setting up config")
    if basedir:
        basedir = pathlib.Path(basedir)
    base_port = random.randint(30001, 40000)
    config = {
        i: {
            "port": base_port + i,
            "dir": (basedir / f"server-{i}") if basedir else None,
        }
        for i in range(count)
    }
    with open("config.yaml", "w") as f:
        YAML().dump(config, f)


@cli.command()
@click.option("--config", default="config.yaml")
@click.option("--message", type=click.INT)
@click.option("--to-server", type=click.INT)
def client(config, message, to_server):
    with open(config) as f:
        config = YAML().load(f)
    if not to_server in config.keys():
        print("You are trying to send to a server that does not exit.")
        sys.exit(1)
    socket = ClientObjectSocket.get_cosclient(config[to_server]["port"])
    socket.send_object(Message(message, 666, to_server))


if __name__ == "__main__":
    cli()
