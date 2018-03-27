
import os
import json
import time
import pika
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

        logging.debug("Check consumer thread '%s' result: %s", self.current_thread.name, "ACK" if result else "NACK")
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


class Connection:

    def get_parameter(self, key, param):
        key = "AMQP_" + key
        if key in os.environ:
            return os.environ.get(key)

        if param in self.amqp_config:
            return self.amqp_config[param]
        raise RuntimeError("Missing '" + param + "' configuration value.")

    def load_configuration(self, config: dict):
        self.amqp_config = config
        self.amqp_username = self.get_parameter('USERNAME', 'username')
        self.amqp_password = self.get_parameter('PASSWORD', 'password')
        self.amqp_vhost    = self.get_parameter('VHOST', 'vhost')
        self.amqp_hostname = self.get_parameter('HOSTNAME', 'hostname')
        port = self.get_parameter('PORT', 'port')
        self.amqp_port     = int(port)

    def connect(self, queues):
        credentials = pika.PlainCredentials(
            self.amqp_username,
            self.amqp_password
        )

        parameters = pika.ConnectionParameters(
            self.amqp_hostname,
            self.amqp_port,
            self.amqp_vhost,
            credentials
        )

        logging.info("Connection to AMQP")
        logging.info(self.amqp_hostname)
        logging.info(self.amqp_port)
        logging.info(self.amqp_vhost)

        # time.sleep(1)
        connection = pika.BlockingConnection(parameters)
        self.connection = connection
        channel = connection.channel()
        logging.info("Connected")
        for queue in queues:
            channel.queue_declare(queue=queue, durable=False)
        self.channel = channel

    def consume(self, queue, callback):
        consumer = BasicConsumer(callback, self.channel, name = "BasicConsumer", daemon = True)
        self.channel.basic_consume(consumer.handle_message,
                      queue=queue,
                      no_ack=False)

        logging.info('Service started, waiting messages ...')
        self.channel.start_consuming()

    def send(self, queue, message):
        self.channel.basic_publish(
            exchange = '',
            routing_key = queue,
            body = message
        )

    def sendJson(self, queue, message):
        logging.info(message)
        encodedMessage = json.dumps(message, ensure_ascii=False)
        self.send(queue, encodedMessage)

    def close(self):
        logging.info("close AMQP connection")
        self.connection.close()
