"""
ExpiryTimer with enough control (I think) to run some test scenarios.

Key functionality is to be able to use servers that don't expire.
"""

import random
import time


class ExpiryTimer:
    def __init__(self, min_time_ms, max_time_ms=None):
        self.min_time_ms = min_time_ms
        self.max_time_ms = max_time_ms if max_time_ms else 2 * min_time_ms
        self.start = None
        self.expiry_time = None
        self.time_fn = time.monotonic
        self.noexpire = False
        self.reset()

    def expired(self):
        if self.noexpire:
            return False
        else:
            return self.time_fn() > self.expiry_time

    def reset(self, *, _interval=None):
        self.start = self.time_fn()
        if _interval:
            self.expiry_time = self.start + _interval
        else:
            self.expiry_time = self.start + (
                random.randint(self.min_time_ms, self.max_time_ms) / 1000
            )

    def _set_manual(self):
        """Timer is not used, have time set by calls to _set_time"""
        self.time_fn = lambda: self.now

    def _set_time(self, time):
        self._set_manual()
        self.now = time

    def _set_noexpire(self, noexpire=True):
        self.noexpire = noexpire
