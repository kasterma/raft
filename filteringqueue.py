import logging
from queue import Queue
from threading import Lock


class FilteringQueue:
    logger = logging.getLogger("filteringqueue")

    def __init__(self, *, drop_filter_fn=None, pass_filter_fn=None):
        self.queue = Queue()
        self.lock = Lock()
        self.update_filter(drop_filter_fn=drop_filter_fn, pass_filter_fn=pass_filter_fn)

    def update_filter(self, *, drop_filter_fn=None, pass_filter_fn=None):
        with self.lock:
            if drop_filter_fn and pass_filter_fn:
                raise FilteringQueueError(
                    "Can only specify one of drop or pass filter."
                )
            if (not drop_filter_fn) and (not pass_filter_fn):
                self.filter_fn = lambda m: False  # drop nothing
                return

            def filter(m):
                """return True if needs to be dropped"""
                if drop_filter_fn:
                    return drop_filter_fn(m)
                if pass_filter_fn:
                    return not pass_filter_fn(m)

            self.filter_fn = filter

    def put(self, m, *args, **kwargs):
        self.logger.debug("Put %s.", str(m))
        with self.lock:
            if self.filter_fn(m):
                self.logger.debug(f"Dropping message {m}.")
            else:
                self.queue.put(m, *args, **kwargs)

    def get(self, *args, **kwargs):
        self.logger.debug("Get")
        try:
            while True:
                m = self.queue.get(*args, **kwargs)
                self.logger.debug("Got %s", str(m))
                with self.lock:
                    if self.filter_fn(m):
                        self.logger.debug(f"Dropping message {m}.")
                    else:
                        return m
                self.logger.debug("Reget b/c of drop")
        except:
            raise
