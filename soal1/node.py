#!/usr/bin/env python
from rabbitmq import RabbitMQ, Consumer, Publisher
import time
import json
import pika
import threading
import sys
from datetime import datetime
from tinydb import TinyDB, Query

QUEUE_URL = '152.118.148.103'
QUEUE_PORT = '5672'
USERNAME = '1306398983'
PASSWORD = '446167'
VHOST = '1306398983'

EX_WRITE = 'EX_WRITE'
EX_READ = 'EX_READ'

DIRECT = 'direct'
FANOUT = 'fanout'

class Node(object):
    def __init__(self, node_id):
        self.node_id = node_id

        self.publisher = Publisher(
            queue_url=QUEUE_URL,
            queue_port=QUEUE_PORT,
            username=USERNAME,
            password=PASSWORD,
            virtual_host=VHOST
        )

        self.consumer = Consumer(
            queue_url=QUEUE_URL,
            queue_port=QUEUE_PORT,
            username=USERNAME,
            password=PASSWORD,
            virtual_host=VHOST
        )

        consume_write_thread = threading.Thread(
            target=self.consume_write
        )
        consume_write_thread.start()

    def attach_sender_id(self, message):
        message = {
            'sender_id': self.node_id,
            'message': message
        }

        return json.dumps(message)

    def callback(self, ch, method, properties, body):
        print('Callback called! body={}'.format(body))
        pass

    def consume_write_callback(self, ch, method, properties, body):
        print('WRITE Callback called! body={}'.format(body))

        self.publish_ack(body)

    def consume_write(self):
        routing_key = 'WRITE_{}'.format(self.node_id)
        self.consumer.consume(
            ex_name=EX_WRITE,
            routing_key=routing_key,
            type=DIRECT,
            callback=self.consume_write_callback
        )

    def publish_ack(self, message):
        routing_key = 'ACK_RELAY'
        self.publisher.publish(
            ex_name=EX_WRITE,
            routing_key=routing_key,
            message=self.attach_sender_id(message)
        )


args = sys.argv

if len(args) > 1:
    node_id = args[1]
    print 'NODE_ID = {}'.format(node_id)
    node = Node(node_id)
else:
    print('Usage: python node.py [NODE_ID]')
