#!/usr/bin/env python
import time
import json
import pika
import threading
from datetime import datetime
from tinydb import TinyDB, Query

class RabbitMQ(object):
    def __init__(self, queue_url, queue_port, username, password, virtual_host):
        self.queue_url = queue_url
        self.queue_port = queue_port
        self.credentials = pika.PlainCredentials(username, password)
        self.virtual_host = virtual_host
        self.connection = None
        self.channel = None
        self.init_connection()

    def init_connection(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.queue_url,
                                      credentials=self.credentials,
                                      virtual_host=self.virtual_host))
        self.channel = self.connection.channel()


class Consumer(RabbitMQ):
    def __init__(self, queue_url, queue_port, username, password, virtual_host):
        super(Consumer, self).__init__(queue_url, queue_port, username, password, virtual_host)

    def consume(self, ex_name, routing_key, type='fanout', callback=None):
        print('Consuming ex: {}, key: {}, type={}'.format(
            ex_name,
            routing_key,
            type
        ))
        self.channel.exchange_declare(exchange=ex_name,
                                      exchange_type=type,
                                      durable=True)

        result = self.channel.queue_declare(exclusive=True)

        queue_name = result.method.queue
        self.channel.queue_bind(exchange=ex_name,
                                queue=queue_name,
                                routing_key=routing_key)

        self.channel.basic_consume(consumer_callback=callback,
                                   queue=queue_name,
                                   no_ack=True)

        self.channel.start_consuming()

class Publisher(RabbitMQ):
    def __init__(self, queue_url, queue_port, username, password, virtual_host, callback=None):
        super(Publisher, self).__init__(queue_url, queue_port, username, password, virtual_host)
        self.callback = callback

    def build_body(self, message):
        body = message
        return json.dumps(body)

    def publish(self, ex_name, routing_key, message, type='fanout', response_consumer=None):
        print('Published ex: {}, key={}, type={}, body={}'.format(
            ex_name, routing_key, type, message
        ))

        self.channel.exchange_declare(exchange=ex_name,
                                      exchange_type=type,
                                      durable=True)

        self.channel.basic_publish(exchange=ex_name,
                                   routing_key=routing_key,
                                   body=message)
