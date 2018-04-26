
import os
import json
import time
import pika
import logging

from amqp_connection.consumer import Consumer

class Connection:

    def __init__(self):
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None

    def run(self, config: dict, in_queue: str, out_queues: list, consumer_callback):
        self._in_queue = in_queue
        self._out_queues = out_queues
        self._consumer_callback = consumer_callback

        self.load_configuration(config)
        self.connect(in_queue)

    def close(self):
        if not self._connection or self._connection.is_closed:
            return
        logging.info('Stopping')
        self._connection.close()
        logging.info('Stopped')

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


    ##################
    ### CONNECTION ###
    ##################

    def connect(self, in_queue, delay = 0):
        while True:
            if delay:
                logging.info("Try connection in %s seconds...", delay)
                time.sleep(delay)

            try:
                self.open_connection()
                self.open_channel()
                self.setup_queues()
                self.start_consuming(in_queue)

            except pika.exceptions.AMQPConnectionError as e:
                logging.error("Connection error: %s", e)
                delay = delay if delay else 10
                self.close()
                continue

            except Exception as e:
                logging.error("An error occurred consuming: %s", e)
                self.close()

            break

    def open_connection(self):
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

        logging.info("**** Connection to AMQP:")
        logging.info(" - %s", self.amqp_hostname)
        logging.info(" - %s", self.amqp_port)
        logging.info(" - %s", self.amqp_vhost)

        self._connection = pika.BlockingConnection(parameters)

    ###############
    ### CHANNEL ###
    ###############

    def open_channel(self):
        logging.info('Creating a new channel')
        self._channel = self._connection.channel()
        self._channel.basic_qos(prefetch_count=1)

    #############
    ### QUEUE ###
    #############

    def setup_queues(self):
        for queue in self._out_queues:
            self.declare_queue(queue)

        self.declare_queue(self._in_queue)

    def declare_queue(self, queue_name):
        logging.info('Declaring queue: %s', queue_name)
        self._channel.queue_declare(queue_name)

    ###############
    ### CONSUME ###
    ###############

    def start_consuming(self, queue_name):
        logging.info('Start consuming on queue %s', queue_name)
        self._consumer_tag = self._channel.basic_consume(self.on_message,
                queue = queue_name,
                no_ack = False)

        logging.info('Service started, waiting messages ...')
        self._channel.start_consuming()

    def process_message(self, channel, basic_deliver, properties, body):
        ack = False
        try:
            ack = self._consumer_callback.__call__(channel, basic_deliver, properties, body)
        except Exception as e:
            logging.error("An error occurred in consumer callback: %s", e)
        return ack

    def on_message(self, channel, basic_deliver, properties, body):
        logging.info('Received message # %s: %s', basic_deliver.delivery_tag, body)

        consumer = Consumer(target=self.process_message, args=(channel, basic_deliver, properties, body))
        consumer.start()
        while consumer.is_alive():
            time.sleep(5)
            self._connection.process_data_events()

        if consumer.get_result() in [None, True]:
            self.acknowledge_message(basic_deliver.delivery_tag)
        else:
            self.negative_acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        logging.info('ACK message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def negative_acknowledge_message(self, delivery_tag):
        logging.info('NACK message %s', delivery_tag)
        self._channel.basic_nack(delivery_tag)

    def stop_consuming(self):
        if self._channel:
            logging.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self._consumer_tag)

    ###############
    ### PUBLISH ###
    ###############

    def publish(self, queue, message):
        logging.info("Publish message to '%s' queue: %s", queue, message)
        self._channel.basic_publish(
            exchange = '',
            routing_key = queue,
            body = message
        )

    def publish_json(self, queue, message):
        encodedMessage = json.dumps(message, ensure_ascii = False)
        self.publish(queue, encodedMessage)
