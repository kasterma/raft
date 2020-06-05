import logging
import queue
import random
from logging.config import dictConfig

import yaml

from kraft import AppendEntries, AppendEntriesReply, Message, RaftServer
from raftlog import LogEntry

with open("logging.yaml") as f:
    dictConfig(yaml.load(f, Loader=yaml.FullLoader))

logger = logging.getLogger("testlogger")


def send_message(s: RaftServer, m):
    """Send message m to s.

    Since the servers have all communication going through these two
    queues we can just put it on its recieving queue.w

    """
    s.recvQueue.put(m)


def get_message(s: RaftServer):
    """Get a message that was send by s.

    Since the servers have all communication going through these two
    queues we can just get from the sending queue.

    """
    try:
        return s.sendQueue.get(block=False)
    except queue.Empty:
        return None


def get_all_messages(s: RaftServer):
    ms = []
    while m := get_message(s):
        ms.append(m)
    return ms


def step_server(s: RaftServer):
    s.state.step()


def gen_config(ct, p=None):
    if not p:
        p = random.randint(
            50000, 60000
        )  # randomly chosen port; should really check are unused
    config = {i: {"port": p + i, "dir": None} for i in range(ct)}
    return config
