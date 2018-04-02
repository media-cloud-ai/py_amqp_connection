
import time
from threading import Thread

class Consumer(Thread):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)

    def get_result(self):
        return self._return

    def join(self):
        Thread.join(self)
        return self.get_result()
