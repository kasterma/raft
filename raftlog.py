# persistence to disk
# flush()
# fsync()

import base64
import hashlib
import logging
import os
import pathlib
import pickle
from threading import Lock
from typing import List

import yaml


class RaftLogException(Exception):
    pass


class LogEntryDigestError(RaftLogException):
    pass


class RaftLogInconsistent(RaftLogException):
    pass


class RaftStoredLogInconsistent(RaftLogException):
    pass


class LogEntry:
    """Entry for in the raft log

    Can be serialized to and from yaml; but b/c we have no faith in reliability
    of disks we add a hash for consistency checking.
    """

    def __init__(self, term, item):
        self.term = term
        self.item = item

    def __repr__(self):
        return f"LogEntry({self.term}, {self.item})"

    def __eq__(self, other):
        return self.term == other.term and self.item == other.item

    def toyaml(self):
        s = f"term: {self.term}\nitem: {base64.b64encode(pickle.dumps(self.item)).decode('ascii')}\n"
        s_digest = hashlib.sha256(bytes(s, "ascii")).hexdigest()
        s_complete = s + f"digest: {s_digest}"
        return s_complete

    @staticmethod
    def fromyaml(s: str):
        last_newline = s.rfind("\n")
        check_digest = hashlib.sha256(bytes(s[: last_newline + 1], "ascii")).hexdigest()
        if not check_digest == s[last_newline + 9 :]:
            raise LogEntryDigestError()
        d = yaml.load(s[:last_newline], Loader=yaml.FullLoader)
        return LogEntry(
            d["term"], pickle.loads(base64.b64decode(bytes(d["item"], "ascii")))
        )


class RaftLog:
    """The Raft Log; if directory=None don't persist to disk.

    The persisted version on disk takes a directory filled with files
    whose names are the indices of the LogEntry that are stored in it,
    e.g. if the log contains [LogEntry(1, 2), LogEntry(1, 3)], then
    the directory contains a file "0" with contents LogEntry(1, 2).toyaml()
    and a file "1" with contents LogEntry(1, 3).toyaml().
    """

    logger = logging.getLogger("raftlog")

    def __init__(self, directory=None):
        """@directory: the directory to store the log items in, if non-empty
             load the log from it"""
        self.log: List[LogEntry] = []
        self.lock = Lock()

        self.directory = directory
        if not self.directory:
            self.logger.warning("Using a raftlog that is not persisted to disk")
        else:
            files = {int(f.name): f for f in self.directory.glob("[0-9]*")}
            indices = sorted(files.keys())
            if not indices == list(range(len(indices))):
                raise RaftStoredLogInconsistent()
            for index in indices:
                with open(files[index]) as f:
                    try:
                        entry = LogEntry.fromyaml(f.read())
                    except LogEntryDigestError as e:
                        raise RaftStoredLogInconsistent(e)
                self.log.append(entry)
        self._invariant("end of init")

    @staticmethod
    def from_list(entries, *args):
        log = RaftLog(*args)
        log.append_entries(0, None, entries)
        return log

    def _invariant(self, message=""):
        """Check internal log consistency."""
        with self.lock:
            for idx in range(len(self.log) - 1):
                if self.log[idx].term > self.log[idx + 1].term:
                    raise RaftLogInconsistent(message)

    def _truncate(self, index):
        """truncate the log to log[:index]"""
        with self.lock:
            if self.directory:
                for idx in range(index, len(self.log)):
                    (
                        self.directory / str(idx)
                    ).unlink()  # does unlink need something like fsync?
            self.log = self.log[:index]

    def _append(self, entries):
        """append to the log to log + entries"""
        with self.lock:
            if self.directory:
                for idx in range(len(entries)):
                    with open(self.directory / str(len(self.log) + idx), "w") as f:
                        f.write(entries[idx].toyaml())
                        f.flush()
                        os.fsync(f.fileno())
            self.log = self.log + entries

    def add_entry(self, entry):
        """Function called on the leader to add an entry"""
        self._append([entry])

    def append_entries(self, index, prev_term, entries: List[LogEntry]):
        if index == 0 or (
            0 < index <= len(self.log) and self.log[index - 1].term == prev_term
        ):
            self._truncate(index)
            self._invariant("after truncate")
            self._append(entries)
            self._invariant("after append")
            return True
        return False

    def __len__(self):
        with self.lock:
            return len(self.log)

    def measure_log(self):
        """The measure of the log used for determining if can vote for a leader"""
        with self.lock:
            term = self.log[-1].term if self.log else -1
        return (term, len(self))

    def __getitem__(self, index):
        with self.lock:
            return self.log[index]

    def __repr__(self):
        with self.lock:
            return str(self.log)

    def __eq__(self, other):
        with self.lock:
            if isinstance(other, list):
                return self.log == other
            else:
                return self.log == other.log
