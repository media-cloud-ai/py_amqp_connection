
import time
import logging

from threading import Thread

class ConsumerProcessThread(Thread):
    """
    Thread that returns a value on join.
    """
    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
        Thread.__init__(self, group, target, name, args, kwargs, daemon=daemon)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)

    def get_result(self):
        return self._return

    def join(self):
        Thread.join(self)
        return self.get_result()


class BasicConsumer(Thread):

    def __init__(self, callback, channel, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
        Thread.__init__(self, group, target, name, args, kwargs, daemon=daemon)
        self.consumer_callback = callback
        self.channel = channel
        self.current_delivery_tag = None
        self.current_thread = None
        self.start()

    def is_processing(self):
        return self.current_thread is not None and self.current_thread.is_alive()

    def is_ready(self):
        return self.current_thread is None and self.current_delivery_tag is None

    def check_current_thread_result(self):
        result = self.current_thread.join()

        logging.debug("Check consumer thread '%s' result: %s", self.current_thread.name, "ACK" if result in [None, True] else "NACK")
        if result in [None, True]:
            self.channel.basic_ack(self.current_delivery_tag)
        else:
            self.channel.basic_nack(self.current_delivery_tag)

        self.reset()

    def run(self):
        while True:
            if not self.is_processing():
                time.sleep(1)
                continue
            self.check_current_thread_result()

    def reset(self):
        logging.debug("Reset consumer thread parameters...")
        self.current_thread = None
        self.current_delivery_tag = None

    def handle_message(self, ch, method, properties, body):
        if self.is_processing():
            # Already processing a message, NACK incoming messages
            time.sleep(1)
            self.channel.basic_nack(method.delivery_tag)
            return

        while not self.is_ready():
            time.sleep(1)

        # Start a new process thread
        try:
            self.current_thread = ConsumerProcessThread(target = self.consumer_callback.__call__,
                name = "ConsumerProcessThread", args = (ch, method, properties, body))
            logging.debug("Start new process thread: %s", self.current_thread)
            self.current_thread.start()
            self.current_delivery_tag = method.delivery_tag

        except Exception as e:
            logging.error("An error occurred in consumer callback: %s", e)
