from collections import defaultdict
from dataclasses import dataclass
from typing import Union


@dataclass
class Command:
    id: str  # uuid identifying the command
    key: str
    val: Union[None, int]  # None means look up


class StateMachine:
    """The replicated state machine

    For now keep command results for ever; maybe later put a TTL on that.
    """

    def __init__(self):
        self.state = defaultdict(int)
        self.cmds = {}  # map from cmd uuid to result

    def execute(self, cmd: Command) -> int:
        """commands have int results; note that if cmd has already been executed return earlier result"""
        try:
            return self.cmds[cmd.id]
        except KeyError:
            if cmd.val:
                self.state[cmd.key] = cmd.val
            self.cmds[cmd.id] = self.state[cmd.key]
            return self.cmds[cmd.id]

    def lookup(self, cmd: Command) -> Union[None, int]:
        """check if cmd has already been executed, then return its value, otherwise return None"""
        return self.cmds.get(cmd.id)
