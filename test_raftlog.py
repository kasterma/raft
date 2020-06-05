from dataclasses import dataclass
from unittest.mock import Mock, call

import pytest

from raftlog import (
    LogEntry,
    LogEntryDigestError,
    RaftLog,
    RaftLogInconsistent,
    RaftStoredLogInconsistent,
)


@dataclass
class TestItem:
    a: int
    b: str


def test_item_through_yaml():
    li1 = LogEntry(1, "hello")
    assert LogEntry(1, "hello") == LogEntry.fromyaml(li1.toyaml())
    assert LogEntry(1, TestItem(23, "testtest")) == LogEntry.fromyaml(
        LogEntry(1, TestItem(23, "testtest")).toyaml()
    )


def test_item_invalid_digest():
    with pytest.raises(LogEntryDigestError):
        LogEntry.fromyaml(LogEntry(1, TestItem(23, "testtest")).toyaml()[:-4])


def test_from_list():
    entries = [LogEntry(1, 1), LogEntry(2, 3)]
    l1 = RaftLog.from_list(entries)
    assert l1 == entries


def test_append_entries():
    l1 = RaftLog()

    assert l1.measure_log() == (-1, 0)

    assert l1.append_entries(0, None, [LogEntry(1, 1)])
    assert l1 == [LogEntry(1, 1)]

    # do the same thing again
    assert l1.append_entries(0, None, [LogEntry(1, 1)])
    assert l1 == [LogEntry(1, 1)]

    # skip index not allowed
    assert not l1.append_entries(2, 1, [LogEntry(1, 1)])

    assert l1.measure_log() == (1, 1)
    assert not l1.append_entries(1, 0, [LogEntry(0, 0)])
    assert not l1.append_entries(2, 1, [LogEntry(0, 0)])
    assert l1 == [LogEntry(1, 1)]

    assert l1.append_entries(1, 1, [LogEntry(1, 1)])
    assert l1 == [LogEntry(1, 1), LogEntry(1, 1)]

    assert l1.measure_log() == (1, 2)
    assert l1.append_entries(1, 1, [LogEntry(1, 2)])
    assert l1 == [LogEntry(1, 1), LogEntry(1, 2)]

    assert l1.append_entries(0, None, [LogEntry(1, 3)])
    assert l1 == [LogEntry(1, 3)]

    assert l1.append_entries(0, None, [LogEntry(1, 4), LogEntry(1, 5)])
    assert l1 == [LogEntry(1, 4), LogEntry(1, 5)]

    # adding entries that don't satisfy the non-decreasing terms
    with pytest.raises(RaftLogInconsistent):
        l1.append_entries(1, 1, [LogEntry(2, 6), LogEntry(1, 5)])

    # Note: we detect this inconsistency, but don't attempt a fix
    with pytest.raises(RaftLogInconsistent):
        l1._invariant()

    # reset l1
    assert l1.append_entries(0, None, [LogEntry(1, 4), LogEntry(1, 5)])
    assert l1 == [LogEntry(1, 4), LogEntry(1, 5)]

    # added entries satisfy the non-decreasing terms but not in combination
    # with what is already there
    with pytest.raises(RaftLogInconsistent):
        l1.append_entries(1, 1, [LogEntry(0, 6), LogEntry(2, 5)])

    # reset l1
    assert l1.append_entries(0, None, [LogEntry(1, 4), LogEntry(1, 5)])
    assert l1 == [LogEntry(1, 4), LogEntry(1, 5)]

    assert l1.append_entries(1, 1, [LogEntry(2, 6), LogEntry(2, 5)])
    assert l1.measure_log() == (2, 3)

    assert l1.append_entries(3, 2, [])
    assert l1.measure_log() == (2, 3)

    assert not l1.append_entries(4, 2, [])
    assert not l1.append_entries(3, 1, [])

    # the paper doesn't quite seem to specify if the truncate should
    # happen in this case, but I believe it does no harm
    assert l1.append_entries(2, 2, [])
    assert l1.measure_log() == (2, 2)


def test_recover_raftlog(tmp_path):
    for i in range(3):
        with open(tmp_path / str(i), "w") as f:
            f.write(LogEntry(i, i).toyaml())
    l1 = RaftLog(tmp_path)
    assert l1.measure_log() == (2, 3)
    assert l1 == [LogEntry(0, 0), LogEntry(1, 1), LogEntry(2, 2)]


def test_recover_raftlog_inconsistent_1(tmp_path):
    # file with index 0 is missing
    for i in range(3):
        with open(tmp_path / str(i + 1), "w") as f:
            f.write(LogEntry(i, i).toyaml())
    with pytest.raises(RaftStoredLogInconsistent):
        RaftLog(tmp_path)


def test_recover_raftlog_inconsistent_2(tmp_path):
    # file with index 1 has invalid digest
    for i in range(3):
        with open(tmp_path / str(i), "w") as f:
            if i == 1:
                f.write(LogEntry(i, i).toyaml()[:-3])
            else:
                f.write(LogEntry(i, i).toyaml())
    with pytest.raises(RaftStoredLogInconsistent):
        RaftLog(tmp_path)


def test_recover_raftlog_2(tmp_path):
    l1 = RaftLog(tmp_path)
    l1.append_entries(0, None, [LogEntry(1, 1), LogEntry(2, 2), LogEntry(3, 3)])

    l2 = RaftLog(tmp_path)
    assert l1 == l2


def test_figure7_tests():
    """suggested tests"""
    entry_to_add = LogEntry(8, "x")
    index_to_add = 11
    prev_term_to_add = 6
    add_args = (index_to_add, prev_term_to_add, [entry_to_add])
    l_a = RaftLog()
    l_a.append_entries(0, None, [LogEntry(i, i) for i in [1, 1, 1, 4, 4, 5, 5, 6, 6]])
    assert not l_a.append_entries(*add_args)

    l_b = RaftLog()
    l_b.append_entries(0, None, [LogEntry(i, i) for i in [1, 1, 1, 4]])
    assert not l_b.append_entries(*add_args)

    l_c = RaftLog()
    l_c.append_entries(
        0, None, [LogEntry(i, i) for i in [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6]]
    )
    assert l_c.append_entries(*add_args)

    l_d = RaftLog()
    l_d.append_entries(
        0, None, [LogEntry(i, i) for i in [1, 1, 1, 4, 4, 5, 5, 6, 6, 7, 7]]
    )
    assert not l_d.append_entries(*add_args)

    l_e = RaftLog()
    l_e.append_entries(0, None, [LogEntry(i, i) for i in [1, 1, 1, 4, 4, 4]])
    assert not l_e.append_entries(*add_args)

    l_f = RaftLog()
    l_f.append_entries(
        0, None, [LogEntry(i, i) for i in [1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3]]
    )
    assert not l_f.append_entries(*add_args)
