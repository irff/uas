#!/usr/bin/env python
from rabbitmq import RabbitMQ, Consumer, Publisher
import time
import json
import pika
import threading
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

class Relay(object):
    def __init__(self):
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

        self.nodes = [1, 2, 3]

        consume_ack_thread = threading.Thread(
            target=self.consume_ack
        )
        consume_ack_thread.start()


    def callback(self, ch, method, properties, body):
        print('Callback called! body={}'.format(body))
        pass

    # Relay menerima pesan ACK dari suatu node dan mem-broadcast ke node-node lain
    def consume_ack_callback(self, ch, method, properties, body):
        print('ACK Callback called! body={}'.format(body))

        body = json.loads(body)
        sender_id = body['sender_id']
        message = body['message']

        self.broadcast(message, sender_id)

    # Relay menunggu adanya pesan ACK dari suatu node untuk dibroadcast ke node-node lain
    def consume_ack(self):
        routing_key = 'ACK_RELAY'
        self.consumer.consume(
            ex_name=EX_WRITE,
            routing_key=routing_key,
            type=DIRECT,
            callback=self.consume_ack_callback
        )

    # Relay mengirimkan pesan WRITE dengan tipe DIRECT ke suatu node
    def publish_write(self, message, receiver_node):
        routing_key = 'WRITE_{}'.format(receiver_node)

        self.publisher.publish(
            ex_name=EX_WRITE,
            routing_key=routing_key,
            message=message,
            type=DIRECT
        )

    def broadcast(self, message, source_node):
        for node in self.nodes:
            if node != source_node:
                routing_key = 'BROADCAST_{}'.format(node)
                self.publisher.publish(
                    ex_name=EX_WRITE,
                    routing_key=routing_key,
                    message=message,
                    type=DIRECT
                )
