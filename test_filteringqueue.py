import queue
from dataclasses import dataclass

from filteringqueue import FilteringQueue


@dataclass
class TestMessage:
    a: int
    b: int


def test_filtering_queue_dropfilter():
    fq = FilteringQueue(drop_filter_fn=lambda m: m.a > 2)
    fq.put(TestMessage(1, 1))
    assert fq.get() == TestMessage(1, 1)
    fq.put(TestMessage(3, 2))
    fq.put(TestMessage(1, 44))
    assert fq.get() == TestMessage(1, 44)


def test_filtering_queue_passfilter():
    fq = FilteringQueue(pass_filter_fn=lambda m: m.a <= 2)
    fq.put(TestMessage(1, 1))
    assert fq.get() == TestMessage(1, 1)
    fq.put(TestMessage(3, 2))
    fq.put(TestMessage(1, 44))
    assert fq.get() == TestMessage(1, 44)


def test_filtering_queue_dropfilter_noblock():
    fq = FilteringQueue(pass_filter_fn=lambda m: m.a <= 2)
    fq.put(TestMessage(3, 3))
    ex = False
    try:
        fq.get(timeout=1)
    except queue.Empty:
        print("expected exception {e}")
        ex = True
    if not ex:
        assert False
