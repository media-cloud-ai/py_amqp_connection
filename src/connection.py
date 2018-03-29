
import os
import json
import time
import pika
import logging

class Connection:

    def __init__(self):
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None

    def run(self, config: dict, in_queues: list, out_queues: list, consumer_callback):
        self._in_queues = in_queues
        self._out_queues = out_queues
        self._consumer_callback = consumer_callback

        self.load_configuration(config)
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        logging.info('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.stop()
        self.close_connection()
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

    def connect(self):
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

        logging.info("Connection to AMQP:")
        logging.info(" - %s", self.amqp_hostname)
        logging.info(" - %s", self.amqp_port)
        logging.info(" - %s", self.amqp_vhost)

        return pika.SelectConnection(parameters,
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, connection):
        logging.info('Connection opened')
        self._connection.add_on_close_callback(self.on_connection_closed)
        self.open_channel()

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            logging.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                           reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        self._connection.ioloop.stop()

        if not self._closing:
            # Create a new connection
            self._connection = self.connect()
            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def close_connection(self):
        logging.info('Closing connection')
        self._connection.close()


    ###############
    ### CHANNEL ###
    ###############

    def open_channel(self):
        logging.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        logging.info('Channel opened')
        self._channel = channel
        self._channel.basic_qos(prefetch_count=1)
        self._channel.add_on_close_callback(self.on_channel_closed)

        for queue in self._in_queues:
            self.setup_queue(queue, self.on_queue_declareok_consume)
        for queue in self._out_queues:
            self.setup_queue(queue, self.on_queue_declareok)

    def on_channel_closed(self, channel, reply_code, reply_text):
        logging.warning('Channel %i was closed: (%s) %s',
                       channel, reply_code, reply_text)
        self._connection.close()

    def on_cancelok(self, unused_frame):
        logging.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def close_channel(self):
        logging.info('Closing the channel')
        self._channel.close()


    #############
    ### QUEUE ###
    #############

    def setup_queue(self, queue_name, callback=None):
        logging.info('Declaring queue: %s', queue_name)
        self._channel.queue_declare(callback, queue_name)

    def on_queue_declareok_consume(self, method):
        self.on_queue_declareok(method)
        self.start_consuming(method.method.queue)

    def on_queue_declareok(self, method):
        logging.info('Queue declared: %s', method.method.queue)


    ###############
    ### CONSUME ###
    ###############

    def start_consuming(self, queue_name):
        logging.info('Issuing consumer related RPC commands')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self._consumer_tag = self._channel.basic_consume(self.on_message, queue_name)

    def on_consumer_cancelled(self, method_frame):
        logging.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, channel, basic_deliver, properties, body):
        logging.info('Received message # %s: %s', basic_deliver.delivery_tag, body)

        ack = False
        try:
            ack = self._consumer_callback.__call__(channel, basic_deliver, properties, body)
        except Exception as e:
            logging.error("An error occurred in consumer callback: %s", e)

        if ack in [None, True]:
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
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)


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
