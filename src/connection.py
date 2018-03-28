
import os
import json
import time
import pika
import logging

from queue import Queue, Empty
from threading import Thread

class BasicConsumer(Thread):
    """
    AMQP connection message consumer thread.
    Handle incoming messages, process consumer's callback and manage acknowledge.
    """
    def __init__(self, callback, messages_queue, results_queue, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
        Thread.__init__(self, group, target, name, args, kwargs, daemon=daemon)
        self.consumer_callback = callback
        self.messages_queue = messages_queue
        self.results_queue = results_queue
        self.stopped = False
        self.start()

    def stop(self):
        self.stopped = True

    def get_message(self):
        try:
            (ch, method, properties, body) = self.messages_queue.get(False)
            return dict(channel=ch, method=method, properties=properties, body=body)
        except Empty:
            None

    def run(self):
        try:
            while not self.stopped:
                message = self.get_message()
                if message is None:
                    time.sleep(1)
                    continue

                channel = message['channel']
                method = message['method']
                properties = message['properties']
                body = message['body']

                logging.debug("Consume message #%s: %s", method.delivery_tag, body)

                result = self.consumer_callback.__call__(channel, method, properties, body)
                logging.debug("Message #%s result: %s", method.delivery_tag, "ACK" if result in [None, True] else "NACK")

                if result in [None, True]:
                    channel.basic_ack(method.delivery_tag)
                else:
                    channel.basic_nack(method.delivery_tag)

                self.results_queue.put(method.delivery_tag)

            logging.info("Consumer stopped.")

        except Exception as e:
            logging.error("An error occurred in consumer callback: %s", e)


class Connection:
    """
    AMQP connection manager
    """

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

        connection = pika.BlockingConnection(parameters)
        self.connection = connection
        channel = connection.channel()
        logging.info("Connected")
        for queue in queues:
            channel.queue_declare(queue=queue, durable=False)
        self.channel = channel

        self.messages_queue = Queue()
        self.results_queue = Queue()

    def handle_message(self, ch, method, properties, body):
        self.messages_queue.put((ch, method, properties, body))

    def get_consumer_result(self):
        try:
            return self.results_queue.get(False)
        except Empty:
            return None

    def consume(self, queue, callback):
        BasicConsumer(callback, self.messages_queue, self.results_queue, name = "ConsumerThread")

        logging.info('Service started, waiting messages ...')
        for method_frame, properties, body in self.channel.consume(queue=queue, no_ack=False):
            self.handle_message(self.channel, method_frame, properties, body)

            while self.get_consumer_result() is None:
                self.connection.process_data_events(5)

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
