
import os
import json
import time
import pika
import logging

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
        self.channel.basic_consume(callback,
                      queue=queue,
                      no_ack=True)

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
