from machine import Command, StateMachine


def test_machine():
    m = StateMachine()

    assert 3 == m.execute(Command("c1", "x", 3))
    assert 4 == m.execute(Command("c2", "x", 4))
    assert 4 == m.execute(Command("c3", "x", None))
    assert 3 == m.execute(Command("c1", None, None))

    assert m.lookup(Command("c1", None, None)) == 3
    assert m.lookup(Command("new", None, None)) is None
