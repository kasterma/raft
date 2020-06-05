import socket
import threading
import time
from unittest import mock

from client import Client
from messages import ClientRequest, ClientResponse
from osocket import ClientObjectSocket
from testutils import *


class MockServer:
    def __init__(self, port, id, answers, sleeps):
        self.id = id
        self.socket = ClientObjectSocket.get_cosserver(port)
        self.running = True
        self.answers = answers
        self.sleeps = sleeps
        self.answer_idx = 0

    def start(self):
        def listen_fn():
            while self.running:
                try:
                    s, _ = self.socket.accept()
                    o = s.recv_object()
                    logger.info(
                        f"MockServer({self.id}) recieved object {o} replying {self.answers[self.answer_idx]}"
                    )
                    time.sleep(self.sleeps[self.answer_idx])  # for triggering timeout
                    s.send_object(self.answers[self.answer_idx])
                    self.answer_idx += 1
                except socket.timeout:
                    pass

        threading.Thread(
            target=listen_fn, name=f"MockServer({self.id})", daemon=True
        ).start()

    def stop(self):
        self.running = False


def test_client():
    port = random.randint(25000, 30000)
    mockserver = MockServer(
        port, 10, [Message(123, 123, -1, ClientResponse(1, None, 1))], [0]
    )
    mockserver.start()
    client = Client({0: {"port": port}})
    with mock.patch("uuid.uuid4", return_value=1) as mock_uuid:
        assert client.request("x", 1) == 1

    mockserver.answer_idx = 0
    # getting the same response but now the command id indicates it is stale
    with mock.patch("uuid.uuid4", return_value=2) as mock_uuid:
        assert client.request("x", 1) is None

    mockserver.answer_idx = 0
    mockserver_noleader = MockServer(
        port + 1, 101, [Message(123, 123, -1, ClientResponse(1, 0, None))], [0]
    )
    mockserver_noleader.start()
    client = Client({0: {"port": port}, 1: {"port": port + 1}})
    client.current_server_id = 1

    with mock.patch("uuid.uuid4", return_value=1) as mock_uuid:
        assert client.request("x", 1) is None  # but get info on leader
        assert client.current_server_id == 0
        assert client.re_request() == 1

    mockserver_noleader.stop()
    mockserver.stop()

    port = random.randint(25000, 30000)
    mockserver = MockServer(port, 100, [ClientResponse(1, None, 1)], [2])
    mockserver.start()
    client = Client({0: {"port": port}})
    with mock.patch("uuid.uuid4", return_value=1) as mock_uuid:
        assert client.request("x", 1) == -66  # expired  TODO: exception

    mockserver.stop()


def test_client_finds_leader():
    # set up 3 servers in a configuration where they are not talking to eachother.
    config = gen_config(3)
    leader = (
        RaftServer(0, config)._set_leader()._set_notimeouts()._set_no_sending().start()
    )
    follower = RaftServer(1, config)._set_follower()._set_notimeouts().start()
    follower.leaderId = 0
    candidate = (
        RaftServer(2, config)
        ._set_candidate()
        ._set_notimeouts()
        ._set_no_sending()
        .start()
    )

    client = Client(config)
    client.current_server_id = 1  # the follower

    assert client.request("x") is None
    assert client.current_server_id == 0

    client.current_server_id = 2
    assert candidate.leaderId is None
    assert client.request("x") is None
    #    assert client.current_server_id in {0, 1}  # picked one randomly
    leader.stop()
    follower.stop()
    candidate.stop()


def test_client_leader():
    config = gen_config(3)
    servers = [RaftServer(i, config) for i in config.keys()]
    servers[0]._set_leader()
    for server in servers:
        server.start()

    client = Client(config)
    client.current_server_id = 0
    assert client.request("x", 9) == None
    assert client.current_server_id == 0
    time.sleep(0.1)
    assert servers[0].commitIndex == 1
    assert servers[0].lastApplied == 0
    assert client.re_request() == 9
    cmd = client.current_cmd
    assert client.request("x") == None  # read also goes through the log
    time.sleep(0.1)
    assert client.re_request() == 9
    assert client.request("x", 12) is None
    time.sleep(0.1)
    assert client.re_request() == 12

    # check that the command id is used to look up old responses
    client.current_cmd = cmd
    assert client.re_request() == 9

    time.sleep(1)

    for server in servers:
        server.stop()

    # commands in the log on all servers
    for server in servers:
        assert len(server.log) == 3
        assert server.log[0].item.key == "x"
        assert server.log[0].item.val == 9

    assert servers[0].commitIndex == 3
    assert servers[0].lastApplied == 2
