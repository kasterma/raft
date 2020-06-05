from time import sleep

from expirytimer import ExpiryTimer


def test_expirytimer():
    et = ExpiryTimer(100)
    assert not et.expired()
    sleep(0.2)
    assert et.expired()

    et.reset()
    assert not et.expired()
    sleep(0.2)
    assert et.expired()

    et._set_noexpire()
    assert not et.expired()
    et._set_noexpire(False)

    et._set_time(10)
    et.reset()
    assert not et.expired()
    et._set_time(11)
    assert et.expired()
    et._set_time(10)
    assert not et.expired()
